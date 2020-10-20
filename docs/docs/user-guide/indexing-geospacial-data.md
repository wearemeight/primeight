# Indexing Geo-spatial data

Cassandra is not famous for his handling of geospatial data, 
it is sometimes hard to query geospatial data in Cassandra.
To make this easy, **primeight** makes use of [Uber's H3 Spatial Index](https://eng.uber.com/h3/) 
to partition data and make it easier to query.

This [Hexagonal Hierarchical Spatial Index](https://eng.uber.com/h3/) translates 
real world latitude and longitude pairs to a 64-bit H3 index, identifying a grid cell.
Since this index is hierarchical, the resolution can go from 0 to 15, 0 being coarsest and 15 being finest.

You can read more about Uber's H3 Spatial index [here](https://eng.uber.com/h3/).
