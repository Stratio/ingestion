Stratio NLP Morphlines
=======================

Stratio LanguageExtractor Morphline
=======================

The languageExtractor command detects the language of a specific header and puts the ISO_639-1 code into another header.

Example 

``` 
{
  languageExtractor {
    input : input_header
    output : output_header
  }
}
``` 

Stratio TopWords Morphline
=======================

The topWords command detects the most frequent word and puts it into a header field. It needs a header that contains the language of the text.

Example 

``` 
{
  topWords {
    language : language_header
    input : input_header
    output : output_header
  }
}
``` 