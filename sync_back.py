"""Pull admin-driven status changes from Feishu back into the local DB.

Run periodically (manually, via cron, or as the first step of a pipeline
run) so that articles marked READY / PUBLISHED / DISCARD in the Feishu
Bitable UI are reflected in articles.db.

The local SQLite store stays the source of truth; this script just
promotes human decisions made in Feishu into the DB.

Usage:
    python sync_back.py
"""

import db
import feishu_sync
from settings.config import FEISHU_SYNC_ENABLED


def main() -> None:
    if not FEISHU_SYNC_ENABLED:
        print(
            "[sync_back] Feishu sync is disabled (FEISHU_APP_ID / "
            "FEISHU_APP_SECRET / FEISHU_APP_TOKEN / FEISHU_TABLE_ID not "
            "all set). Nothing to do."
        )
        return

    db.init_db()
    conn = db._get_conn()
    rows = conn.execute("SELECT unique_id, status FROM articles").fetchall()
    local = {r["unique_id"]: r["status"] for r in rows}
    print(f"[sync_back] local rows: {len(local)}")

    diffs = feishu_sync.pull_status_changes(local)
    if not diffs:
        print("[sync_back] no status changes from Feishu.")
        return

    applied = 0
    for d in diffs:
        try:
            db.update_status(d["unique_id"], d["new_status"])
            print(
                f"[sync_back] {d['unique_id'][:8]}...: "
                f"{d['old_status']} -> {d['new_status']}"
            )
            applied += 1
        except Exception as exc:
            print(
                f"[sync_back failed] {d['unique_id'][:8]}... "
                f"{d['old_status']} -> {d['new_status']}: {exc}"
            )

    print(f"[sync_back] applied {applied}/{len(diffs)} status changes")


if __name__ == "__main__":
    main()
