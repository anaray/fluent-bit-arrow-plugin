# Fluent Bit Arrow Output Plugin
Fluent Bit Arrow plugin is a [Fluent Bit output plugin](https://docs.fluentbit.io/manual/v/1.3/output), which writes parsed data in [Apache Arrow](https://arrow.apache.org/) format. This plugin uses [Apache Flight RPC](https://arrow.apache.org/docs/format/Flight.html) to write the data to a remote Flight RPC server.

## Why use a Arrow Output Plugin?
The primary use of this plugin is in data ingestion for data processing systems or data warehouses. Data ingested and stored in such systems normally would have  would have fixed __structure__ or __schema__. We could find a example usecase in IoT usecase, where sensors emit certain reading/measurements periodically. Given below is a madeup measurement containing *timestamp, sensor-id, measurement, location-id*

```bash
2023-01-28T13:22:01Z sensor01 58.00 101
2023-01-28T13:22:02Z sensor02 57.00 102
2023-01-28T13:22:03Z sensor03 56.00 103
```

Using Apache Arrow permits system to have a common data format to work with, avoid unnecessary serialization and de-serialization and there by reducing the computational cost and improves processing speed in general.
More details about these can be found in Apache Arrow [documentation](https://arrow.apache.org/overview/).

## Use and Configuration

Fluent Bit is configured using using a configuration file, more details can be found in Fluent Bit's official [documentation](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file). 

