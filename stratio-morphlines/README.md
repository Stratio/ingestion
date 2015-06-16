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
* GeoIP: Command that works as the kite one. It will save the iso code and the longitude-latitude pair in two header fields.
* GeoLocateAirports: Get the longitude and latitude of an airport from its airport code (from origin and destination).
* NLP: Command that detects the language of a specific header and puts the ISO_639-1 code into another header.
* WikipediaCleaner: Command that cleans the wikipedia markup from a text.
* CheckpointFilter: Get the last checkpoint value by parametrized handler and filter records by paramentrized field 
too. Periodically update checkpoints values.

