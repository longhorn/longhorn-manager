# 0.11.0

The `State` field is no longer just an `int`, but a separate `struct`.

It still implements the `Stringer` and `GoStringer` interfaces, so if you were just displaying the value, it should continue working as before.

For comparison purposes, the `State.Raw` field should be used now. It has all the same values as before + 2 new (`Undefined` and `Idle`).
