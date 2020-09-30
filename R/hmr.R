#' @title hmrtest2
#' @description runs hadoop map reduce in R
#' @description hmr runs a chunk-wise Hadoop job.
#' @description hpath and hinput define HDFS file path and input source.
#' @param input input data - see details
#' @param output output path (optional)
#' @param map chunk compute function (map is a misnomer)
#' @param reduce chunk combine function
#' @param job.name name of the job to pass to Hadoop
#' @param aux either a character vector of symbols names or a named list of values to push to the compute nodes
#' @param formatter formatter to use. It is optional in hmr if the input source already contains a formatter definition. See below for details on how to sepcify separate formatters.
#' @param autoformatter provides two alternative methods of detection if formatter is not specified. If TRUE then the functions uses dynamic detection and FALSE performs the detection in advance.
#' @param formsep specifies the separator for the formatter, default is comma
#' @param packages character vector of package names to attach on the compute nodes
#' @param reducers optional integer specifying the number of parallel jobs in the combine step. It is a hint in the sense that any number greater than one implies independence of the chunks in the combine step. Default is to not assume independence.
#' @param wait logical, if TRUE then the command returns after the job finished, otherwise the command returns after the job has been submitted
#' @param hadoop.conf optional string, path to the hadoop configuration directory for submission
#' @param hadoop.opt additional Java options to pass to the job - named character vectors are passed as -D<name>=<value>, unnamed vectors are collapsed. Note: this is only a transitional interface to work around deficiencies in the job generation and should only be used as a last measure since the semantics is implementation specific and thus not prtable across systems.
#' @param R command to call to run R on the Hadoop cluster
#' @param verbose logical, indicating whether the output sent to standard error and standard out from hadoop should be printed to the console.
#' @param persistent logical, if TRUE then an ROctopus job is started and the mapper is executed in "hot" ROctopus instances instead of regular R. The results in that case are ROctopus URLs.
#' @param overwrite logical, if TRUE then the output directory is first deleted before the job is started.
#' @param use.kinit logical, if TRUE automatically invokes kinit(realm=getOption("hmr.kerberos.realm")) before running any Hadoop commands.
#' @param path HDFS path
#' @return hmr returns the HDFS path to the result when finished.
#' @return hpath returns a character vector of class "HDFSpath"
#' @return hinput returns a subclass "hinput" of "HDFSpath" containing the additional "formatter" attribute.
#' @details hmr creates and runs a Hadoop job to perform chunkwise compute + combine. The input is read using chunk.reader, processed using the formatter function and passed to the map function. The result is converted using as.output before going back to Hadoop. The chunkwise results are combined using the reduce function - the flow is the same as in the map case. Then result is returned as HDFS path. Either map or reduce can be identity (the default).
#' @details If the formatter if omitted then the format is taken from input object (if it has one) or the default formatter (mstrsplit with '\t' as key spearator, '|' as column separator) is used. If formater is a function then the same formatter is used for both the map and reduce steps. If separate formatters are required, the formatter can be a list with the entries map and/or reduce specifying the corresponding formatter function.
#' @details hpath tags a string as HDFS path. The sole purpose here is to distiguish local and HDFS paths.
#' @details hinput creates a subclass of HDFSpath which also contains the definition of the formatter for that path. The default formatter honors default Hadoop settings of '\t' as the key/value separator and '|' as the field separator.
#' @note Requires properly installed Hadoop client. The installation must either be in /usr/lib/hadoop or one of HADOOP_HOME, HADOOP_PREFIX environment variables must be set accordingly.
#' @export
hmrtest2 <- function(input, output, map=identity, reduce=identity, job.name, aux, formatter, autoformatter=NULL, formsep=',', packages=loadedNamespaces(), reducers,
                     wait=TRUE, hadoop.conf, hadoop.opt, R="R", verbose=TRUE, persistent=FALSE, overwrite=FALSE,
                     use.kinit = !is.null(getOption("hmr.kerberos.realm"))) {
  .rn <- function(n) paste(sprintf("%04x", as.integer(runif(n, 0, 65536))), collapse='')
  if (missing(output)) output <- hpath(sprintf("/tmp/io-hmr-temp-%d-%s", Sys.getpid(), .rn(4)))
  if (missing(job.name)) job.name <- sprintf("RCloud:iotools:hmr-%s", .rn(2))
  if (!inherits(input, "HDFSpath")) stop("Sorry, you have to have the input in HDFS for now")
  ## only use kinit if use.kinit and all tickets expired
  if (isTRUE(use.kinit) && all(krb5::klist()$expired)) krb5::kinit(realm=getOption("hmr.kerberos.realm"))
  map.formatter <- NULL
  red.formatter <- NULL
  #if (missing(formatter) && inherits(input, "hinput"))
  #map.formatter <- attr(input, "formatter")
  if (!missing(formatter)) {
    if (is.list(formatter)) {
      map.formatter <- formatter$map
      red.formatter <- formatter$reduce
    } else map.formatter <- red.formatter <- formatter
  }
  ## this could be true with formatter=list(reduce=...) in which case we still use the one from input
  #if (is.null(map.formatter) && inherits(input, "hinput"))
  #map.formatter <- attr(input, "formatter")
  if (missing(formatter) || is.null(map.formatter)) {
    if (isTRUE(autoformatter)) {
      coltypes = function(r, sep=formsep, nsep=NA, header=TRUE) {
        s = mstrsplit(r, sep=sep, skip=header)
        apply(s, 2, function(x) class(type.convert(x, as.is = TRUE)))
      }
      map.formatter <- function(x) dstrsplit(x, coltypes(x), sep=formsep)
    }
    else if (autoformatter==FALSE) { #isFALSE introduced after 3.5
      guess <- function(path, sep='|', nsep='\t', nrowsClasses=25L, header=TRUE) {
        f <- pipe(paste("hadoop fs -cat", shQuote(path)), "rb")
        cr <- chunk.reader(f)
        r <- read.chunk(cr, 1e6)
        subset = mstrsplit(r, sep=sep, nsep=nsep, nrows=nrowsClasses, skip=header)
        colClasses = rep(NA_character_, ncol(subset))
        for (i in 1:ncol(subset))
          colClasses[i] = class(type.convert(subset[,i],as.is=TRUE))
        # If all NA's, R makes it logical; better to be character
        index = which(apply(!is.na(subset), 2, sum) == 0)
        if (length(index))
          colClasses[index] = "character"
        if (header) {
          col_names = mstrsplit(r, sep=sep, nsep=nsep, nrows=1)
          if ((length(col_names) - 1 == length(colClasses)) && !is.na(nsep))
            col_names = col_names[-1]
          names(colClasses) = col_names
        }
        close(f)
        rm(list=c("cr", "r", "f"))
        function(x) dstrsplit(x, colClasses, sep=sep, nsep=nsep, skip=header)
      }
      map.formatter <- guess(paste0(input,"/*"), sep=formsep, nsep=NA)
    }
    else map.formatter <- .default.formatter
  }

  if (is.null(red.formatter))
    red.formatter <- .default.formatter

  h <- .hadoop.detect()
  hh <- h$hh
  hcmd <- h$hcmd
  sj <- h$sj
  if (!length(sj))
    stop("Cannot find streaming JAR - set HADOOP_STREAMING_JAR or make sure you have a complete Hadoop installation")

  e <- new.env(parent=emptyenv())
  if (!missing(aux)) {
    if (is.list(aux)) for (n in names(aux)) e[[n]] <- aux[[n]]
    else if (is.character(aux)) for (n in aux) e[[n]] <- get(n) else stop("invalid aux")
  }
  e$map.formatter <- map.formatter
  e$red.formatter <- red.formatter
  e$map <- map
  e$reduce <- reduce
  e$load.packages <- packages
  f <- tempfile("hmr-stream-dir")
  dir.create(f,, TRUE, "0700")
  owd <- getwd()
  on.exit(setwd(owd))
  setwd(f)
  save(list=ls(envir=e, all.names=TRUE), envir=e, file="stream.RData")
  map.cmd <- if (isTRUE(persistent)) {
    paste0("-mapper \"",R," --slave --vanilla -e 'hmr:::run.ro()'\"")
  }
  else {
    if (identical(map, identity)) "-mapper cat" else if (is.character(map)) paste("-mapper", shQuote(map[1L])) else paste0("-mapper \"",R," --slave --vanilla -e 'hmr:::run.map()'\"")
  }
  reduce.cmd <- if (identical(reduce, identity)) "" else if (is.character(reduce)) paste("-reducer", shQuote(reduce[1L])) else paste0("-reducer \"",R," --slave --vanilla -e 'hmr:::run.reduce()'\"")
  extraD <- if (missing(reducers)) "" else paste0("-D mapred.reduce.tasks=", as.integer(reducers))
  if (!missing(hadoop.opt) && length(hadoop.opt)) {
    hon <- names(hadoop.opt)
    extraD <- if (is.null(hon))
      paste(extraD, paste(as.character(hadoop.opt), collapse=" "))
    else
      paste(extraD, paste("-D", shQuote(paste0(hon, "=", as.character(hadoop.opt))), collapse=" "))
  }

  hargs <- paste(
    "-D", "mapreduce.reduce.input.limit=-1",
    "-D", shQuote(paste0("mapred.job.name=", job.name)), extraD,
    paste("-input", shQuote(input), collapse=' '),
    "-output", shQuote(output),
    "-file", "stream.RData",
    map.cmd, reduce.cmd)
  cfg <- ""
  if (!missing(hadoop.conf)) cfg <- paste("--config", shQuote(hadoop.conf)[1L])

  ## FIXME: to support Hadoop 1 it woudl have to be rmr?
  if (overwrite) system(paste(shQuote(hcmd), "fs", "-rm", "-r", shQuote(output)), ignore.stdout = TRUE, ignore.stderr = FALSE)
  h0 <- paste(shQuote(hcmd), cfg, "jar", shQuote(sj[1L]))
  cmd <- paste(h0, hargs)
  system(cmd, wait=wait, ignore.stdout = !verbose, ignore.stderr = !verbose)
  output
}
