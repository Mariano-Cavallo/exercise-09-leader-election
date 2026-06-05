import os
import threading
import time
import requests

NODE_ID = int(os.environ.get("NODE_ID", "1"))
PEERS = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))
ELECTION_TIMEOUT = float(os.environ.get("ELECTION_TIMEOUT", "3"))

_leader_id: int | None = None
_leader_url: str | None = None
_election_in_progress = False
_lock = threading.Lock()

# Maps NODE_ID -> base URL for all peers including self
def _peer_id(url: str) -> int:
    """Extract node id from a peer URL like http://node-2:8080 -> 2."""
    host = url.split("//")[-1].split(":")[0]  # e.g. "node-2"
    return int(host.split("-")[-1])


def _all_peers_with_ids() -> list[tuple[int, str]]:
    return [(_peer_id(u), u) for u in PEERS]


def _higher_peers() -> list[tuple[int, str]]:
    return [(pid, url) for pid, url in _all_peers_with_ids() if pid > NODE_ID]


def _send(url: str, path: str, payload: dict, timeout: float = ELECTION_TIMEOUT) -> bool:
    try:
        r = requests.post(f"{url}{path}", json=payload, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def start_election() -> None:
    """Initiate a Bully election: send ELECTION to all higher-ID nodes."""
    global _election_in_progress, _leader_id, _leader_url
    with _lock:
        if _election_in_progress:
            return
        _election_in_progress = True

    higher = _higher_peers()
    got_ok = False

    for _, url in higher:
        ok = _send(url, "/election/message", {"sender_id": NODE_ID})
        if ok:
            got_ok = True

    if not got_ok:
        # No higher node responded — we win
        declare_victory()
    else:
        # Wait for a COORDINATOR message; if none arrives, retry
        time.sleep(ELECTION_TIMEOUT * 2)
        with _lock:
            still_no_leader = _leader_id is None or _leader_id == NODE_ID
        if still_no_leader:
            with _lock:
                _election_in_progress = False
            start_election()

    with _lock:
        _election_in_progress = False


def handle_election_message(_sender_id: int) -> None:
    """Respond OK to a lower-ID node and start our own election."""
    # We are higher — take over
    threading.Thread(target=start_election, daemon=True).start()


def declare_victory() -> None:
    """Announce self as the new leader to all peers."""
    global _leader_id, _leader_url
    with _lock:
        _leader_id = NODE_ID
        _leader_url = None  # self is leader

    for _, url in _all_peers_with_ids():
        _send(url, "/election/coordinator", {"leader_id": NODE_ID})


def set_leader(leader_id: int) -> None:
    """Called when we receive a COORDINATOR message from the new leader."""
    global _leader_id, _election_in_progress
    with _lock:
        _leader_id = leader_id
        _election_in_progress = False


def get_leader() -> dict:
    with _lock:
        return {"leader_id": _leader_id, "node_id": NODE_ID}


def _leader_alive() -> bool:
    if _leader_id is None:
        return False
    if _leader_id == NODE_ID:
        return True  # we are the leader
    for pid, url in _all_peers_with_ids():
        if pid == _leader_id:
            try:
                r = requests.get(f"{url}/health", timeout=ELECTION_TIMEOUT)
                return r.status_code == 200
            except Exception:
                return False
    return False


def heartbeat_check() -> None:
    """Periodically check if the current leader is alive; trigger election if not."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with _lock:
            in_election = _election_in_progress
        if not in_election and not _leader_alive():
            threading.Thread(target=start_election, daemon=True).start()


def start_background_tasks() -> None:
    """Start the heartbeat thread. Call once at app startup."""
    t = threading.Thread(target=heartbeat_check, daemon=True)
    t.start()
