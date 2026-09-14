## 1. How the workload is distributed and paced during writes

This document explains how the benchmark maps **devices to tables and clients**, the order in which
each client writes, and how timestamps are assigned. Understanding this is necessary for sizing a
run, judging whether the load is saturating the server, and diagnosing "this table has no data"
style problems.

> Note: this describes the **synthetic data generation** path
> (`BENCHMARK_WORK_MODE=testWithDefaultPath` or `generateDataMode`).
> `verificationWriteMode` / `verificationQueryMode` replay a CSV data set and distribute differently.

### 1.1. The three mapping layers

Writing involves three layers, all resolved **once at startup** and never changed afterwards:

```
device  ──①──▶  table  ──②──▶  database  ──③──▶  client (thread)
```

#### ① device → table

Computed in the `DeviceSchema` constructor through `MetaUtil.mappingId`. With the default
`SG_STRATEGY=mod`:

```
tableId = deviceId % IoTDB_TABLE_NUMBER
```

So table `t` owns the devices `{t, t + M, t + 2M, ...}` where `M = IoTDB_TABLE_NUMBER`, and every
table owns exactly `DEVICE_NUMBER / IoTDB_TABLE_NUMBER` devices.

#### ② Device reordering (important)

`MetaUtil.sortDeviceId()` **reorders devices so that they cluster by table**, producing a flat list
`deviceIds`:

```
[all devices of table 0, all devices of table 1, ..., all devices of table M-1]
```

The purpose is to guarantee that **all devices in one batch belong to the same table** (see the
comment on `MetaUtil.sortDeviceId`). With `GROUP_NUMBER=1` the database bucketing step does not
disturb that order.

#### ③ device → client

`MetaUtil.distributeDevices()` takes **contiguous slices** of the reordered `deviceIds` and hands
them to the clients in order:

```
base devices per client = DEVICE_NUMBER / DATA_CLIENT_NUMBER
the remaining DEVICE_NUMBER % DATA_CLIENT_NUMBER devices go one each to the first clients
```

Because the devices are already clustered by table, a contiguous slice is equivalent to **a slice of
tables**: as long as `DEVICE_NUMBER / IoTDB_TABLE_NUMBER` divides evenly into
`DEVICE_NUMBER / DATA_CLIENT_NUMBER`, each client gets a whole number of tables and **no two clients
share a table**.

> `distributeDevices()` is called once for schema clients and once for data clients
> (`GenerateMetaDataSchema.createMetaDataSchema`). When `SCHEMA_CLIENT_NUMBER == DATA_CLIENT_NUMBER`
> the two get identical device slices.

### 1.2. Write pacing inside one client

For `testWithDefaultPath`, the main loop of `GenerateDataMixClient` is:

```java
for (; taskProgress.getLoopIndex() < config.getLOOP(); taskProgress.incrementLoopIndex()) {
    Operation operation = operationController.getNextOperationType();
    if (operation == Operation.INGESTION) {
        ingestionOperation();      // ← one call walks EVERY device of this client
    }
    ...
}
```

and `ingestionOperation()` is:

```java
for (int i = 0; i < clientDeviceSchemas.size(); i += config.getDEVICE_NUM_PER_WRITE()) {
    IBatch batch = dataWorkLoad.getOneBatch();
    if (checkBatch(batch)) {
        dbWrapper.insertOneBatchWithCheck(batch);
    }
}
insertLoopIndex++;
```

That is:

- **one `LOOP` iteration = one full sweep over the devices this client owns = one "pass"**
- within a pass the devices are written **serially**: device → device → … → last device, then
  `insertLoop++` and it starts over from the first
- which table a write lands on is determined by which table the device belongs to; since devices are
  clustered by table, the trajectory is **the 100 devices of table A → the 100 of table B → …**

With `DEVICE_NUM_PER_WRITE > 1`, every `DEVICE_NUM_PER_WRITE` devices are merged into one batch (one
tablet carrying several devices), but the traversal order is unchanged.

### 1.3. Clients run in parallel, but their phases drift

The `DATA_CLIENT_NUMBER` clients are **aligned at the start by a `CyclicBarrier`, then run
independently**:

- at the starting instant they really do all begin together
- but there is **no phase synchronization at all** between them; none waits for another
- each client's pace depends on its data distribution and on server load, so **their phases drift
  apart** after a while

So "which 20 tables are being written right now" has a definite answer only at the starting instant;
during the run it drifts. The one thing that is always true is that **each client only ever writes
its own 50 tables**.

### 1.4. Timestamp assignment

In `SyntheticDataWorkLoad` (with `IS_CLIENT_BIND=true`, the default):

```java
long rowOffset = insertLoop * config.getBATCH_SIZE_PER_WRITE();
...
records.add(new Record(getCurrentTimestamp(rowOffset), generateOneRow(...)));
```

Note that `rowOffset` depends **only on `insertLoop`, not on `deviceIndex`**. This means:

- **within one pass, all devices of that client write exactly the same
  `BATCH_SIZE_PER_WRITE` timestamps**
- pass `p` covers `rowOffset ∈ [p*B, (p+1)*B)`, so consecutive passes are contiguous
- total rows per device = `LOOP * BATCH_SIZE_PER_WRITE`

With regular frequency (`IS_REGULAR_FREQUENCY=true`, the default) the timestamp is
`START_TIMESTAMP * precision + POINT_STEP * rowOffset + POINT_STEP`.

### 1.5. Worked example

Take "1000 tables, 100 devices per table, 10 columns, concurrency 20":

```
IoTDB_TABLE_NUMBER    = 1000
DEVICE_NUMBER         = 1000 × 100 = 100000      # tables × devices per table
SENSOR_NUMBER         = 10
DATA_CLIENT_NUMBER    = 20
LOOP                  = 60000
BATCH_SIZE_PER_WRITE  = 1000
DEVICE_NUM_PER_WRITE  = 1                        # default
```

Plugging these into the rules above:

| Quantity | Computation | Result |
|---|---|---|
| devices per client | 100000 / 20 | 5000 |
| tables per client | 5000 / 100 | **50** |
| client 0 owns | — | tables 0..49 |
| client 1 owns | — | tables 50..99 |
| … | … | … |
| client 19 owns | — | tables 950..999 |
| rows per device | 60000 × 1000 | 60 million |
| points per device | 60M × 10 | 600 million |
| **total points** | 100000 × 10 × 60000 × 1000 | **6 × 10¹³ (60 trillion)** |

It is **not** "20 tables written simultaneously, then move on to the next 20". It is:

> 20 clients, each bound to 50 tables, each writing its own tables' devices **serially** starting
> from the first device of its first table. After finishing its 50 tables (= one `LOOP` iteration)
> it starts over from the beginning, for `LOOP` rounds in total.

### 1.6. Relevant configuration

| Property | Meaning | Default |
|---|---|---|
| `DEVICE_NUMBER` | total number of devices | 6000 |
| `SENSOR_NUMBER` | columns (measurements) per device | 200 |
| `IoTDB_TABLE_NUMBER` | number of tables in table model | 1 |
| `GROUP_NUMBER` | number of databases | 1 |
| `DATA_CLIENT_NUMBER` | write concurrency (threads) | 20 |
| `SCHEMA_CLIENT_NUMBER` | schema-registration threads | 20 |
| `LOOP` | passes per client | 100 |
| `BATCH_SIZE_PER_WRITE` | rows per batch (per device) | 100 |
| `DEVICE_NUM_PER_WRITE` | devices merged into one batch | 1 |
| `SG_STRATEGY` | device→table mapping (mod/hash/div) | mod |
| `IS_CLIENT_BIND` | statically bind devices to clients | true |
| `IS_SENSOR_TS_ALIGNMENT` | all columns share one timestamp | true |
| `POINT_STEP` | interval between adjacent rows (ms) | 5000 |

### 1.7. Constraints and validation

`ConfigDescriptor.checkDeviceNumPerWrite()` validates the following at startup and exits with an
error if any fails:

- `DEVICE_NUM_PER_WRITE > 0`
- when `DEVICE_NUM_PER_WRITE > 1`:
  - `IS_SENSOR_TS_ALIGNMENT` must be `true`
  - only IoTDB / DolphinDB are supported
  - the device count allocated to each client must be divisible by it
- in table model:
  - `DEVICE_NUMBER % IoTDB_TABLE_NUMBER == 0`
  - devices per table must be divisible by `DEVICE_NUM_PER_WRITE`
  - `DATA_CLIENT_NUMBER % GROUP_NUMBER == 0` (keeps a client writing to a single database)

`MetaUtil.distributeDevices()` additionally verifies in table model that a client's devices never
span more than one database.

### 1.8. Common misconceptions

**"More concurrency means all tables fill up faster"**
No. Clients are statically bound to tables: client 0 will never write table 999. To change how many
tables each client owns, change `DATA_CLIENT_NUMBER` or the table count — not the scheduling.

**"A larger DEVICE_NUM_PER_WRITE reduces table switching"**
It does merge devices and reduce RPC count, but **a batch never spans tables** (that is exactly what
the `sortDeviceId()` reordering guarantees). Memory use also grows linearly with
`DEVICE_NUM_PER_WRITE × BATCH_SIZE_PER_WRITE`.

**"Total points = devices × columns × LOOP"**
You are missing `BATCH_SIZE_PER_WRITE`. Note that outside `generateDataMode` this property controls
both the row count *and* the time span: it is the number of rows per pass and therefore the time
span between consecutive passes.
