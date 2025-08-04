# References

This page contains references and citations for technologies, standards, and research that Tellus builds upon.

## Bibliography

```{bibliography}
:filter: docname in docnames
```

## Related Projects

### Data Management Systems
- **fsspec** - Filesystem interfaces for Python
- **intake** - Declarative data loading for scientific workflows
- **Dask** - Parallel computing library with data management features

### Workflow Engines
- **Snakemake** - Workflow management system for reproducible analyses  
- **Nextflow** - Data-driven computational pipeline framework
- **Apache Airflow** - Platform for workflow orchestration

### Cloud Storage Libraries
- **boto3** - AWS SDK for Python
- **google-cloud-storage** - Google Cloud Storage client library
- **azure-storage-blob** - Azure Blob Storage client library

### SSH/SFTP Libraries
- **paramiko** - Pure Python SSH/SFTP implementation
- **fabric** - High-level SSH library for Python

## Standards and Protocols

### File Transfer Protocols
- **SSH File Transfer Protocol (SFTP)** - RFC 4251-4256
- **HTTP/HTTPS** - RFC 7230-7237
- **Amazon S3 REST API** - AWS S3 API Reference

### Data Formats
- **NetCDF** - Network Common Data Form
- **HDF5** - Hierarchical Data Format version 5
- **Zarr** - Chunked, compressed, N-dimensional arrays

## Acknowledgments

Tellus development was inspired by and builds upon the excellent work of:

- The **fsspec** community for filesystem abstraction patterns
- The **Snakemake** project for workflow integration ideas  
- The **Rich** library for beautiful terminal interfaces
- The **Click** library for CLI development patterns

## External Documentation

### Cloud Provider Documentation
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Azure Blob Storage Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/)

### Protocol Documentation
- [SSH Protocol Specifications](https://tools.ietf.org/html/rfc4251)
- [SFTP Protocol Specification](https://tools.ietf.org/html/draft-ietf-secsh-filexfer-02)

### Library Documentation
- [fsspec Documentation](https://filesystem-spec.readthedocs.io/)
- [Paramiko Documentation](https://docs.paramiko.org/)
- [boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)