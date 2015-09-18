Stratio GeoIp Morphline
=======================

The stratio geoIP command works as the [kite one](http://kitesdk.org/docs/0.12.0/kite-morphlines/morphlinesReferenceGuide.html#/geoIP). It will save the iso code and the longitude-latitude pair in two header fields.


- inputField: The name of the input field that contains zero or more IP addresses. Default:n/a
- database: The relative or absolute path of a Maxmind database file on the local file system. Default: GeoLite2-City.mmdb
    Example: /path/to/GeoLite2-City.mmdb

Example:

``` 
{
  sgeoIP {
    input : log_host
    database : "/home/cesar/flume/GeoLite2-City.mmdb"
    isoCodeOutput : log_iso_code
    longitudeLatituteOutput : log_longitude_latitude
  }
}
``` 