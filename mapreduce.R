library(parallel)
library(stringr)
library(magrittr)

mapreduce = function(map_func, reduce_func, mc.cores=4)
{
    stopifnot(is.function(map_func))
    stopifnot(is.function(reduce_func))

    function(x)
    {
        # Split - None 
        # Map

        map_res = mclapply(x, map_func, mc.cores=mc.cores) %>%
                  unlist(recursive=FALSE)

        # Shuffle

        nm = names(map_res) %>% unique()
        shuf_res = lapply(nm, function(x) unlist(map_res[names(map_res) == x]) ) %>%
                   setNames(nm)

        # Reduce

        reduce_res = mclapply(shuf_res, reduce_func, mc.cores=mc.cores)

        return(reduce_res)
    }
}


word_count_map = function(x)
{
    x %>% tolower() %>%
    str_replace_all("[[:punct:]]","") %>%
    str_split(" ") %>% 
    unlist() %>% 
    str_trim() %>%
    .[. != ""] %>%
    table() %>%
    as.list()
}

word_count_map_no_short = function(x)
{
    x %>% tolower() %>%
    str_replace_all("[[:punct:]]","") %>%
    str_split(" ") %>% 
    unlist() %>% 
    str_trim() %>%
    .[. != ""] %>%
    .[nchar(.) > 3] %>%
    table() %>%
    as.list()
}

work_count_map = function(file)
{
    readLines(file) %>%
    paste(collapse=" ") %>%
    word_count_map_no_short()
}


count_words = mapreduce(word_count_map, sum)
count_words_no_short = mapreduce(word_count_map_no_short, sum)

count_words_all = mapreduce(work_count_map, sum)


ex_text = list("the quick","brown","the fox the")






f_word_count("Hamlet")
f_word_coutn("Richard III")