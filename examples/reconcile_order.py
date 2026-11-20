import time

from grvt import resolve_order_sync


def reconcile_order(client, sub_account_id, client_order_id):
    for attempt in range(5):
        result = resolve_order_sync(
            client,
            sub_account_id=sub_account_id,
            client_order_id=client_order_id,
        )
        if result.status == "found":
            return result  # Inspect the observed order; PENDING is not final.
        if attempt < 4:
            time.sleep(1)
    return result  # Still unknown. Save the ids for investigation; do not resend.
