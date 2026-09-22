"""容器容量上限的端到端测试。

覆盖：
* PUT /containers/{id}/capacity 设置/修改/取消容量上限（null 不限制）；
* 上限必须不小于当前体积；查询返回 capacity / remaining / utilization；
* 批量提交在顺序模拟的每一步检查“目标当时体积 + 转入量”，任一目标超限即
  整批拒绝，错误 details 带条目序号、目标与预计超出量，状态/版本/幂等/审计不变；
* 同批先转出再转入按当时余量判断；预演与提交判定一致、错误响应一致；
* 失败不写幂等记录、同键可重试；已成功请求的幂等重放保持原结果；
* 旧库启动时幂等迁移 containers.capacity 列（回填 NULL）。
"""
import sqlite3

from helpers import container, sample, volume_of

from app.db import connect


def _get(client, cid):
    r = client.get(f"/containers/{cid}")
    assert r.status_code == 200, r.text
    return r.json()


def _set_capacity(client, cid, value):
    return client.put(f"/containers/{cid}/capacity", json={"capacity": value})


def _version_of(client, cid):
    return _get(client, cid)["version"]


def _batch(client, transfers, key=None):
    body = {"transfers": transfers}
    if key is not None:
        body["idempotency_key"] = key
    return client.post("/transfers/batch", json=body)


# --------------------------------------------------------------------------- #
# 查询：默认无上限；capacity / remaining / utilization
# --------------------------------------------------------------------------- #
def test_new_containers_have_null_capacity_fields(client):
    sample(client, "S1", "10")
    container(client, "V1")
    for cid, vol in (("S1", "10.000000000"), ("V1", "0")):
        body = _get(client, cid)
        assert body["volume"] == vol
        assert body["capacity"] is None
        assert body["remaining"] is None
        assert body["utilization"] is None


def test_set_capacity_then_query_remaining_and_utilization(client):
    sample(client, "S1", "3")
    r = _set_capacity(client, "S1", "8")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["volume"] == "3.000000000"
    assert body["capacity"] == "8.000000000"
    assert body["remaining"] == "5.000000000"
    assert body["utilization"] == "0.375000"

    # GET 与 PUT 返回同结构
    assert _get(client, "S1") == body

    # 空容器：利用率 0、剩余等于上限
    container(client, "V1")
    body = _set_capacity(client, "V1", 5).json()
    assert body["capacity"] == "5.000000000"
    assert body["remaining"] == "5.000000000"
    assert body["utilization"] == "0.000000"


def test_capacity_equal_to_volume_is_full(client):
    sample(client, "S1", "10")
    body = _set_capacity(client, "S1", "10").json()
    assert body["remaining"] == "0.000000000"
    assert body["utilization"] == "1.000000"


def test_zero_capacity_empty_container_is_utilization_one(client):
    container(client, "V1")
    body = _set_capacity(client, "V1", "0").json()
    assert body["capacity"] == "0.000000000"
    assert body["remaining"] == "0.000000000"
    # 0/0 无数学定义：零容量容器一律视为“已满”
    assert body["utilization"] == "1.000000"


def test_set_null_clears_capacity(client):
    sample(client, "S1", "10")
    assert _set_capacity(client, "S1", "10").status_code == 200
    r = _set_capacity(client, "S1", None)
    assert r.status_code == 200
    body = r.json()
    assert body["capacity"] is None
    assert body["remaining"] is None
    assert body["utilization"] is None
    assert _get(client, "S1") == body


def test_capacity_change_does_not_touch_version_or_volume(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _batch(client, [{"source": "S1", "destination": "V1", "volume": "4"}])
    assert _version_of(client, "V1") == 1

    assert _set_capacity(client, "V1", "100").status_code == 200
    assert _set_capacity(client, "V1", None).status_code == 200
    assert _version_of(client, "V1") == 1
    assert volume_of(client, "V1") == "4.000000000"


# --------------------------------------------------------------------------- #
# 设置容量的校验
# --------------------------------------------------------------------------- #
def test_capacity_below_current_volume_rejected_and_unchanged(client):
    sample(client, "S1", "10")
    r = _set_capacity(client, "S1", "9.999999999")
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "capacity_below_volume"
    assert "S1" in err["message"]

    # 容量未修改：仍是不限制
    body = _get(client, "S1")
    assert body["capacity"] is None
    assert body["version"] == 0


def test_invalid_capacity_values_rejected(client):
    sample(client, "S1", "10")
    for bad in ("-1", "1.0000000001"):
        r = _set_capacity(client, "S1", bad)
        assert r.status_code == 422, r.text
        assert r.json()["error"]["code"] == "invalid_capacity"
    assert _get(client, "S1")["capacity"] is None


def test_set_capacity_unknown_container_404(client):
    r = _set_capacity(client, "GHOST", "10")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "unknown_container"


def test_set_capacity_on_sample_container_works(client):
    # 初始样品也是容器，同样可设上限（此后只出不进）
    sample(client, "S1", "10")
    assert _set_capacity(client, "S1", "10").status_code == 200


# --------------------------------------------------------------------------- #
# 批量提交：容量约束、顺序模拟、整批原子
# --------------------------------------------------------------------------- #
def test_batch_rejected_when_destination_exceeds_capacity(client):
    sample(client, "S1", "20")
    container(client, "V1")
    assert _set_capacity(client, "V1", "10").status_code == 200

    r = _batch(client, [{"source": "S1", "destination": "V1", "volume": "11"}])
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "capacity_exceeded"
    assert err["details"] == {
        "index": 1,
        "destination": "V1",
        "overflow": "1.000000000",
    }
    assert "第 1 条转移" in err["message"]

    # 整批不落库：状态与版本不变
    assert volume_of(client, "S1") == "20.000000000"
    assert volume_of(client, "V1") == "0"
    assert _version_of(client, "V1") == 0


def test_batch_capacity_check_uses_then_current_volume_after_prior_steps(client):
    sample(client, "S1", "30")
    container(client, "V1")
    container(client, "V2")
    assert _set_capacity(client, "V1", "10").status_code == 200

    # V1 先装入 8
    assert _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "8"}]
    ).status_code == 200

    # 同批先转出 5（V1: 8 -> 3，释放 5 空间），再转入 7（3 + 7 = 10，恰好到顶）
    r = _batch(
        client,
        [
            {"source": "V1", "destination": "V2", "volume": "5"},
            {"source": "S1", "destination": "V1", "volume": "7"},
        ],
    )
    assert r.status_code == 200, r.text
    assert volume_of(client, "V1") == "10.000000000"
    assert volume_of(client, "V2") == "5.000000000"
    assert volume_of(client, "S1") == "15.000000000"

    # 对照：不先转出时转入 7 必超（10 + 7 > 10，超出 7）
    r = _batch(client, [{"source": "S1", "destination": "V1", "volume": "7"}])
    assert r.status_code == 422
    assert r.json()["error"]["details"] == {
        "index": 1,
        "destination": "V1",
        "overflow": "7.000000000",
    }


def test_capacity_failure_at_later_index_reports_index_and_whole_batch_rolls_back(client):
    sample(client, "S1", "20")
    container(client, "V1")
    container(client, "V2")
    assert _set_capacity(client, "V2", "5").status_code == 200

    r = _batch(
        client,
        [
            {"source": "S1", "destination": "V1", "volume": "2"},
            {"source": "S1", "destination": "V2", "volume": "6"},
        ],
    )
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "capacity_exceeded"
    assert err["details"]["index"] == 2
    assert err["details"]["destination"] == "V2"
    assert err["details"]["overflow"] == "1.000000000"
    assert "第 2 条转移" in err["message"]

    # 第一条也随整批回滚
    assert volume_of(client, "S1") == "20.000000000"
    assert volume_of(client, "V1") == "0"
    assert volume_of(client, "V2") == "0"
    assert _version_of(client, "S1") == 0


def test_capacity_failure_leaves_no_audit_record(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _set_capacity(client, "V1", "5")

    r = _batch(client, [{"source": "S1", "destination": "V1", "volume": "6"}])
    assert r.status_code == 422

    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM audit_records").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM audit_record_containers").fetchone()[0]
            == 0
        )
    finally:
        conn.close()

    # 成功的受限批次后审计链依然完整可校验（容量设置本身不写审计）
    assert _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "5"}]
    ).status_code == 200
    assert _set_capacity(client, "V1", None).status_code == 200
    assert _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "5"}]
    ).status_code == 200
    verify = client.get("/audit/verify").json()
    assert verify["valid"] is True
    assert verify["records"] == 2


def test_zero_capacity_does_not_accept_any_inflow_but_outflow_works(client):
    sample(client, "S1", "10")
    container(client, "V1")
    container(client, "V2")
    _set_capacity(client, "V1", "0")

    # 零容量：任何转入（哪怕 1）都拒绝
    r = _batch(client, [{"source": "S1", "destination": "V1", "volume": "1"}])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "capacity_exceeded"
    assert r.json()["error"]["details"]["overflow"] == "1.000000000"

    # 提高上限后可装入；再降到 0 被拒绝（当前体积 > 0）
    _set_capacity(client, "V1", "5")
    assert _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "5"}]
    ).status_code == 200
    r = _set_capacity(client, "V1", "0")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "capacity_below_volume"

    # 零容量下限不允许设置，但已有的 5 容量下可以全部转出
    assert _batch(
        client, [{"source": "V1", "destination": "V2", "volume": "5"}]
    ).status_code == 200
    assert volume_of(client, "V1") == "0"
    # 腾空后容量仍是 5，此时可以改成 0
    assert _set_capacity(client, "V1", "0").status_code == 200


# --------------------------------------------------------------------------- #
# 预演：容量校验一致、响应一致、不落库
# --------------------------------------------------------------------------- #
def test_preview_enforces_capacity_and_matches_batch_error_body(client):
    sample(client, "S1", "20")
    container(client, "V1")
    container(client, "V2")
    _set_capacity(client, "V2", "5")
    transfers = [
        {"source": "S1", "destination": "V1", "volume": "2"},
        {"source": "S1", "destination": "V2", "volume": "6"},
    ]

    preview = client.post("/transfers/preview", json={"transfers": transfers})
    batch = _batch(client, transfers)
    assert preview.status_code == batch.status_code == 422
    assert preview.json() == batch.json()
    assert preview.json()["error"]["code"] == "capacity_exceeded"
    assert preview.json()["error"]["details"]["index"] == 2

    assert volume_of(client, "S1") == "20.000000000"
    assert volume_of(client, "V1") == "0"
    assert volume_of(client, "V2") == "0"


def test_preview_reports_capacity_fields_in_final_states(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _set_capacity(client, "V1", "8")

    r = client.post(
        "/transfers/preview",
        json={
            "transfers": [
                {"source": "S1", "destination": "V1", "volume": "6"},
            ]
        },
    )
    assert r.status_code == 200, r.text
    finals = {c["container_id"]: c for c in r.json()["containers"]}
    v1 = finals["V1"]
    assert v1["volume"] == "6.000000000"
    assert v1["capacity"] == "8.000000000"
    assert v1["remaining"] == "2.000000000"
    assert v1["utilization"] == "0.750000"
    # 不限制的容器对应字段为 null
    assert finals["S1"]["capacity"] is None
    assert finals["S1"]["remaining"] is None
    assert finals["S1"]["utilization"] is None

    # 预演不落库
    assert volume_of(client, "V1") == "0"
    assert _version_of(client, "V1") == 0


def test_preview_capacity_fields_match_state_after_real_commit(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _set_capacity(client, "V1", "8")
    transfers = [{"source": "S1", "destination": "V1", "volume": "6"}]

    preview = client.post("/transfers/preview", json={"transfers": transfers}).json()
    assert _batch(client, transfers).status_code == 200
    for expected in preview["containers"]:
        actual = _get(client, expected["container_id"])
        assert actual == expected


# --------------------------------------------------------------------------- #
# 幂等：失败不留痕、同键可重试；成功重放保持原结果
# --------------------------------------------------------------------------- #
def test_capacity_failure_writes_no_idempotency_record_and_key_can_retry(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _set_capacity(client, "V1", "3")
    transfers = [{"source": "S1", "destination": "V1", "volume": "4"}]

    # 超限（0 + 4 > 3）：失败不写幂等记录
    r = _batch(client, transfers, key="cap-key")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "capacity_exceeded"

    conn = connect()
    try:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = ?",
                ("cap-key",),
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()
    assert volume_of(client, "S1") == "10.000000000"

    # 同键在放宽上限后重试：首次成功
    _set_capacity(client, "V1", "5")
    r2 = _batch(client, transfers, key="cap-key")
    assert r2.status_code == 200
    assert r2.headers.get("Idempotency-Replayed") is None
    assert volume_of(client, "V1") == "4.000000000"


def test_replayed_success_keeps_original_result_even_if_now_over_capacity(client):
    sample(client, "S1", "10")
    container(client, "V1")
    _set_capacity(client, "V1", "10")
    transfers = [{"source": "S1", "destination": "V1", "volume": "4"}]

    r1 = _batch(client, transfers, key="cap-replay")
    assert r1.status_code == 200
    assert r1.json() == {
        "applied": 1,
        "message": "已依次提交 1 条转移",
        "versions": {"S1": 1, "V1": 1},
    }

    # 另一次无键转移把 V1 装到 8；此时重放“转入 4”在当前状态下会超上限，
    # 但幂等重放直接返回首次结果，不重新校验容量
    assert _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "4"}]
    ).status_code == 200
    assert volume_of(client, "V1") == "8.000000000"

    r2 = _batch(client, transfers, key="cap-replay")
    assert r2.status_code == 200
    assert r2.headers.get("Idempotency-Replayed") == "true"
    assert r2.json() == r1.json()
    # 体积与版本未因重放变化
    assert volume_of(client, "V1") == "8.000000000"
    assert _version_of(client, "V1") == 2


# --------------------------------------------------------------------------- #
# 旧库迁移：init_db 幂等补齐 containers.capacity 列
# --------------------------------------------------------------------------- #
def test_init_db_migrates_capacity_column_on_legacy_database(tmp_path, monkeypatch):
    legacy_path = tmp_path / "legacy.db"

    # 最老的 schema：没有 version，也没有 capacity
    raw = sqlite3.connect(legacy_path)
    try:
        raw.executescript(
            """
            CREATE TABLE containers (
                id      TEXT PRIMARY KEY,
                created INTEGER NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE components (
                container_id TEXT NOT NULL REFERENCES containers(id) ON DELETE CASCADE,
                source_id    TEXT NOT NULL,
                amount       TEXT NOT NULL,
                PRIMARY KEY (container_id, source_id)
            );
            INSERT INTO containers (id) VALUES ('OLD');
            INSERT INTO components (container_id, source_id, amount)
                VALUES ('OLD', 'OLD', '5.500000000');
            """
        )
        raw.commit()
    finally:
        raw.close()

    monkeypatch.setattr("app.config.settings.database_path", str(legacy_path))

    from app.db import connect, init_db

    init_db()  # 一次启动：补齐 version + capacity
    init_db()  # 再次启动必须幂等

    conn = connect()
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(containers)")}
        assert {"version", "capacity"} <= cols
        row = conn.execute(
            "SELECT version, capacity FROM containers WHERE id = 'OLD'"
        ).fetchone()
        # 旧容器：版本回填 0，容量回填 NULL（不限制）
        assert row[0] == 0
        assert row[1] is None
    finally:
        conn.close()
