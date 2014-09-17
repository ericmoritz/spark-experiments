Spark books example
=====================

This is an example of a simple join of two data sources with the idea that the combined data set will be
loaded into a service database for fast quering.

The sample dataset takes line oriented JSON for the google books
service and joins it with a simulated assets on ISBN.

It then sets the asset_id to the books data and outputs the books as line oriented JSON.


