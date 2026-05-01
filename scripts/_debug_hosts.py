# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Quick debug helper: dump SHOW HOSTS output verbatim."""

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


def main() -> int:
    cfg = Config()
    cfg.max_connection_pool_size = 4
    pool = ConnectionPool()
    pool.init([("graphd0", 9669)], cfg)
    session = pool.get_session("root", "nebula")
    try:
        result = session.execute("SHOW HOSTS")
        print("succ:", result.is_succeeded(), "err:", result.error_msg())
        print("col_size:", result.col_size())
        print("row_size:", result.row_size())
        print("keys:", result.keys())
        for i, row in enumerate(result.rows()):
            print(f"row{i} len={len(row.values)} types={[type(v).__name__ for v in row.values]}")
            print(f"  raw={row.values}")
    finally:
        session.release()
    pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
