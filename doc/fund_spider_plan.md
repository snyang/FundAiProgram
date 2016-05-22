# Process

[TOC]

1. keywords
    * request : request content from www
    * save : save content to files
    * read : read content from files
    * store : write content to database
* configuration file: fund_spider.json
    * request_last_succeed_date
    * request_fail_funds
* request and save fund information from today to the last end date
    * need to re-request failed requests
    * save the end date to the configuration file when finished
    * when save, use a better format (json) than requested content
    * the save file name
        * history\_&lt;fundcode&gt;\_&lt;date&gt;.json
        * fund_company
        * company\_&lt;companycode&gt;.json
        * manager
        * fund_manager
* read and store
* calculate
    