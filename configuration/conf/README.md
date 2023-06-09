# Some Configuration Item in config.properties

## DEVICE_NUM_PER_WRITE

Abbreviated as `DNW` below.

### Meaning

The number of devices included in a single write.

### Combination with Other Parameters

When `DNW`=1, the combination effect should be exactly the same as before adding `MultiDeviceBatch`. 

So we only analyze the case where `DNW`>1.

- With `DB_SWITCH`:
  - Database version: Currently only supports `IoTDB-013`.
  - Insert mode: `SESSION_BY_RECORDS` mode allows `DNW`>1, `SESSION_BY_RECORD` and `SESSION_BY_TABLET` modes only allow `DNW`=1, and an error will be reported if the parameters do not match.
- With `BATCH_SIZE_PER_WRITE`:
  - `BATCH_SIZE_PER_WRITE` means the total number of records written in a single write, and `DNW` determines how these records are evenly distributed to multiple devices.
  - `BATCH_SIZE_PER_WRITE` must be divisible by `DNW`.
- With `IS_CLIENT_BIND`: True and false are both supported.
- With `IS_SENSOR_TS_ALIGNMENT`: This configuration item is currently not supported as false, but there will be no error and it will be treated as true.