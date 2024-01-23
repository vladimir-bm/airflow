create table if not exists t_wiki_pages
(
    pageid bigint,
    ns     integer,
    title  varchar(1024)
)
    distributed by (pageid);
