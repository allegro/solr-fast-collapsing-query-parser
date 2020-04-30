# Solr Fast Collapsing Query Plugin #
It is a Solr plugin with a query parser to faster group documents for field collapsing in search results.
As the built-in Collapsing Query Parser our collapsing query parser groups documents (collapsing the result set) according to 
your parameters.

## Requirements
 * Solr version > 7.2 (tested with 7.2.1 - 8.4.0)
 * Solr running in standalone mode 
 * Solr Cloud is also supported only for one shard collections to have consistent grouping.  

## Motivation
The initial motivation for creating this plugin was to use more effective field collapsing then the one available in the Solr 
codebase https://lucene.apache.org/solr/guide/8_4/collapse-and-expand-results.html. We tried to optimize the requests’ 
performance by using a priority queue during filter query phase.  

## Limitations
In our approach we have a limitation. We do not allow to have different sort expression for returning documents and for choosing 
best document in the group. As well we allow to group only by a given field, not by a given expression that could be calculated 
using document fields.     

## Basic concepts
FastCollapsingQueryPlugin consists of:
 * `FastCollapsingQueryFilter`
 * `FastCollapsingNumFoundSearchComponent`
 
The idea behind FastCollapsingQueryPlugin is to use a post-filter (`FastCollapsingQueryFilter`) as the last
filter during the request processing phase, which will collapse documents found. 
Then using search compoment (`FastCollapsingNumFoundSearchComponent`) will fix proper number of documents in the search response.

### `FastCollapsingQueryFilter`
`FastCollapsingQueryFilter` is a Solr post-filter. In Solr terminology, a filter is a piece of
code which decides, whether the document matches search criteria and should be included
in the response. A post-filter will be executed after regular filters, thanks to this it works on 
limited set of documents, already filtered by previous filters.

 `FastCollapsingQueryFilter` is really a post filter.  This parser collapses the result set to a single 
document per group before it forwards the result set to the rest of the search components.

#### The FastCollapsingQueryParser accepts the following local parameters:

* field

The field that is being collapsed on. The field must be a single valued String, Int or Float-type of field.

* exactCount

Allows to calculate the exact number of found collapsed items.
If it set to true, then in `FastCollapsingQueryFilter` it is counted how many collapsed items is found by curent query.
Then this value is used by `FastCollapsingNumFoundSearchComponent` to return properly counted number of collapsed results. 
If exactCount is set to false, then `FastCollapsingNumFoundSearchComponent` will return number of all results (not collapsed).  
By default it is set to false.

* cost 

You can also use the cost option to control the order in which non-cached filter queries are evaluated. 
This allows you to order less expensive non-cached filters before expensive non-cached filters.
For very high cost filters, if cache=false and cost>=100 and the query implements the PostFilter interface, 
a Collector will be requested from that query and used to filter documents after they have matched the main 
query and all other filter queries. There can be multiple post filters; they are also ordered by cost.

#### The FastCollapsingQueryParser accepts the following request parameters:

* sort

Selects the group head document for each group based on which document comes first according to the specified sort string.
If none are specified, the group head document of each group will be selected based on the highest scoring document in that group. The default is none. 

* rows

Sets the initial size of the collapse data structures

### Exceptions in FastCollapsingQueryFilter
FastCollapsingQueryFilter is using slower algorithms for requests:
 * with cursorMark request parameter, that allows pagination using cursors https://lucene.apache.org/solr/guide/8_4/pagination-of-results.html
 * with request parameter rows=0, that allow only to count items returned by query
 * without sort request parameter. 
  

### `FastCollapsingNumFoundSearchComponent`

`FastCollapsingNumFoundSearchComponent` is a search component (piece of code which executes after request processing, 
but before sending the response). A search component is used to return proper number of documents in the search response. 
The problem of our algorithm is the wrong number of results. The number of results in the response is the number of documents 
returned by our filter to the TopDocsCollector collector where this number is calculated. However, we decided to return from 
filter only the requested number of best documents. So, for instance, if we search for 30 best offers, we will get in response 
30 documents and the total number of results will amount to 30 also. Unfortunately, this prevents us from building navigation 
that presents the number of result pages. In order to handle the correct number of documents available for the given criterion, 
we calculate the number of documents consumed by the filter and save it in a request context variable. Then at SearchComponent 
we change the number of returned results in the Response class.
 
## Installation

1. Add JAR file to Solr's classpath https://lucene.apache.org/solr/guide/7_2/lib-directives-in-solrconfig.html
For Solr cloud: https://lucene.apache.org/solr/guide/8_4/solr-plugins.html

2. Add to `solrconfig.xml` following code

    ```xml
    <queryParser name="fastCollapse" class="pl.allegro.search.solr.qparser.FastCollapsingQueryParserPlugin"/>

    <searchComponent name="collapseHits" class="org.apache.solr.search.FastCollapsingNumFoundSearchComponent"/>
   
    <requestHandler name="/select" class="solr.SearchHandler">
        <arr name="components">
            <str>query</str>
            <str>collapseHits</str>
        </arr>
    </requestHandler>   
    ```
    Changing solrconfig in Solr Cloud:
    ```
    curl http://SOLR_URI/solr/CORE/config -H 'Content-type:application/json' -d 
    ' {"add-queryparser":{"name":"fastCollapse","class":"pl.allegro.search.solr.qparser.FastCollapsingQueryParserPlugin" }}'
    
    curl http://SOLR_URL/solr/CORE/config -H 'Content-type:application/json' -d 
    ' {"add-searchcomponent":{"name":"collapseHits","class":"org.apache.solr.search.FastCollapsingNumFoundSearchComponent" }}'
    
    curl http://SOLR_URL/solr/CORE/config -H 'Content-type:application/json' -d 
    ' {"update-requesthandler":{"name":"/select","class":"solr.SearchHandler","components": ["query","collapseHits"] }}'
    ```
    
3. Example of running a query
 
    Each of those components may be registered under any valid name.
    * The name of the `FastCollapsingQueryParserPlugin` (which is a factory for `FastCollapsingQueryFilter`) 
    will be reflected in Solr URL (you will use it in requests to activate the plugin) 
        <pre>http://localhost:8080/solr/core_name/select?q=*:*&fq={!fastCollapse+field%3Dvariant_hash+cost%3D300}</pre> 

## Build
`./gradlew clean build`


## License
This software is published under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
