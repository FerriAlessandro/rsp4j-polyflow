# Examples

This module contains some implementation of stream processors created with the RSP4J APIs.
There are two main examples: [Relational](src/main/java/relational) and [Graph](src/main/java/graph/jena)

Another package is present in the module: [shared](src/main/java/shared), which contains some implementations general enough to be shared by both examples.

The Relational and Graph modules have a similar structure:

- content: implementation of the Content interface, used by the windows to store data points
- datatypes: custom data types used in the examples 
- examples: the entry point of the examples, in which components are instantiated and linked together
- operatorsimpl: specific operators implementations used in the examples
- sds: SDS and Time Varying implementations
- stream: data stream generators used in the examples

