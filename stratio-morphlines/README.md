Stratio Morphlines
==================

Stratio Morphlines add some useful transformations to the defaults Kite SDK. 

Stratio Morphlines consists of several modules:

* Commons
    - Calculator: Make operations between fields and/or static numbers. 
    - FieldFilter: Filter fields including or excluding.
    - ReadXml: Read xml from header or body, and apply XPath expression.
    - RelationalFilter: Drop fields if they don't accomplish a relational condition.
    - Rename: Rename a field.
    - TimeFilter: Filter a time field between specified dates.
    - ContainsAnyOf: Command that succeeds if all field values of the given named fields contains any of the given values and fails otherwise. Multiple fields can be named, in which case a logical AND is applied to the results. 
* GeoIP: 
* NLP: 
* WikipediaCleaner: 
* CheckpointFilter: Get the last checkpoint value by parametrized handler and filter records by paramentrized field 
too. Periodically update checkpoints values.

