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
  - Insert mode: `SESSION_BY_RECORDS` mode allows `DNW`>1, `SESSION_BY_RECORD` and `SESSION_BY_TABLET` modes only allow `DNW`=1, and an error will be reported if the parameters do not match("The combination of DEVICE_NUM_PER_WRITE and insert-mode is not supported").
- With `BATCH_SIZE_PER_WRITE`: `BATCH_SIZE_PER_WRITE` refers to the number of rows written to a single device in a single write operation. The total number of rows written in a single operation is `DNW` × `BATCH_SIZE_PER_WRITE`.
- With `IS_CLIENT_BIND`: True and false are both supported.
- With `IS_SENSOR_TS_ALIGNMENT`: This configuration item is currently not supported as false.