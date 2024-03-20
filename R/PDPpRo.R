#' Update DOMO Personalized Data Permission (PDP)
#'
#' This function allows you to easily add, update, and delete PDP policies based on a DOMO dataset
#' @param client_id A client_id that can be created on the developer.domo.com page.
#' @param secret A secret that can created on the developer.domo.com page.
#' @param ds_id An alpha-numeric string that uniquely identifies a DOMO dataset, can be found on the address bar. The dataset must include `Dataset ID`, `Policy Name`, `Policy Column`, `User ID`, and `Policy Value` fields.
#' @param parallel Provides an option to use parallel feature from the "Foreach" package.
#' @examples PDPpRo(client_id = client_id,
#'   secret = secret,
#'   ds_id = '58dfaba9-c3c4-4099-b7ef-dc2ca222cb24',
#'   parallel = TRUE)
#' @export


PDPpRo <- function(client_id, secret, ds_id, parallel = FALSE) {

  # Connection to DOMO
  domo <- pRoDomo::Domo(client_id = client_id, secret = secret)

  # Check Required Packages
  if (!requireNamespace("magrittr", quietly = TRUE)) {stop("Package \"magrittr\" must be installed to use this function.", call. = FALSE)}
  if (!requireNamespace("rlist", quietly = TRUE)) {stop("Package \"rlist\" must be installed to use this function.", call. = FALSE)}
  if (!requireNamespace("dplyr", quietly = TRUE)) {stop("Package \"dplyr\" must be installed to use this function.", call. = FALSE)}
  if (!requireNamespace("data.table", quietly = TRUE)) {stop("Package \"data.table\" must be installed to use this function.", call. = FALSE)}
  if (!requireNamespace("foreach", quietly = TRUE)) {stop("Package \"foreach\" must be installed to use this function.", call. = FALSE)}


  # Check For Parallelism
  if(parallel == TRUE & parallel::detectCores() <= 1) {stop('Can only do `parallel` with at least 2 cores.', call. = FALSE)}


  # Data
  pdpData <- domo$ds_query(ds_id, "select `Dataset ID`, `Policy Name`, `Policy Column`, `User ID`, `Policy Value` from table")
  pdpDs <- pdpData %>%  dplyr::filter(!is.na(`Dataset ID`) & `Dataset ID` != '' ) %>% dplyr::select(`Dataset ID`) %>% unique()
  if(nrow(pdpDs) == 0) {stop('No `Dataset ID` can be found', call. = FALSE)}


  # Functions
  `%dopar%` <- foreach::`%dopar%`
  `%!in%` <- Negate(`%in%`)
  `%!like%` <- Negate(data.table::`%like%`)
  extractPdp <- function(x) {
    if(length(x)==0) {break}
    for (i in 1:length(x)) {
      if(length(x[[i]]$users) == 0){users <- dplyr::tibble(users = '')} else{users <- dplyr::tibble(users = x[[i]]$users) %>% dplyr::mutate(users = as.character(users)) %>% dplyr::arrange(users)} # Extract Users
      if(length(x[[i]]$filters) == 0){filters <- dplyr::tibble(column = '', values = '')} else{filters <- x[[i]]$filters %>% rlist::list.stack() %>% dplyr::select(column, values) %>% dplyr::mutate(values = as.character(values)) %>% dplyr::arrange(values)}
      x[[i]] <- dplyr::tibble(`Policy ID` = x[[i]]$id, `Policy Name` = x[[i]]$name) %>% merge(users) %>% merge(filters) %>% dplyr::rename(`Policy Column` = column, `User ID` = users, `Policy Value` = values)}

    y <- dplyr::bind_rows(x)
  }
  createPdpList <- function(x){
    if('Policy ID' %in% colnames(x)){id <- as.integer(x$`Policy ID`)} else{id <- NULL}
    filters <- list()
    users <- list()
    longFilters <- x$`Policy Value` %>% strsplit('|', fixed = TRUE) %>% unlist()
    longUsers <- x$`User ID` %>% strsplit('|', fixed = TRUE) %>% unlist()
    for (i in 1:length(longFilters)) {filters[[i]] <- list(column = x$`Policy Column`, values = list(longFilters[i]), operator = 'EQUALS', not = FALSE) } # Create Filters
    for (i in 1:length(longUsers)) {users[[i]] <- as.integer(longUsers) } # Create Users
    pdpList <- list(id = id, type = 'user', name = x$`Policy Name`, filters = filters, users = users, virtualUsers = list(), groups = list())
    if('Policy ID' %!in% colnames(x)){pdpList$id <- NULL}
    return(pdpList)
  }


  # Parallel is TRUE
  if(parallel == TRUE){

    # Create clusters
    n.cores <- parallel::detectCores() - 1
    my.cluster <- parallel::makeCluster(n.cores, type = "PSOCK")
    doParallel::registerDoParallel(cl = my.cluster)

    foreach::foreach(a = 1:nrow(pdpDs), .packages = c('magrittr', 'dplyr')) %dopar% {
      dsID <- pdpDs$`Dataset ID`[a]

      # Current PDP List
      curPolicy <- domo$pdp_list(ds = dsID)
      curPolicyLong <- extractPdp(curPolicy) %>% dplyr::filter(`Policy Name` %!like% 'AA - Restricted' & `Policy Name` != 'All Rows') %>% dplyr::mutate(cur = 1)
      curPolicyWide <- curPolicyLong %>% dplyr::arrange(`Policy Name`, `User ID`, `Policy Value`) %>% dplyr::group_by(`Policy ID`, `Policy Name`, `Policy Column`) %>% dplyr::summarise(`User ID` = paste(unique(`User ID`), collapse = '|'), `Policy Value` = paste(unique(`Policy Value`), collapse = '|'))

      # Correct PDP List
      corPolicyLong <- pdpData %>% dplyr::filter(`Dataset ID` == dsID) %>% select(-`Dataset ID`) %>% dplyr::mutate(`User ID` = as.character(`User ID`), `Policy Value` = as.character(`Policy Value`)) %>% dplyr::left_join(curPolicyWide %>% dplyr::select(`Policy ID`, `Policy Name`) %>% unique()) %>% dplyr::mutate(cor = 1)
      corPolicyWide <- corPolicyLong %>% dplyr::arrange(`Policy Name`, `User ID`, `Policy Value`) %>% dplyr::group_by(`Policy Name`, `Policy Column`) %>% dplyr::summarise(`User ID` = paste(unique(`User ID`), collapse = '|'), `Policy Value` = paste(unique(`Policy Value`), collapse = '|'))

      # IF NO current policies can be found on the dataset
      if(nrow(curPolicyWide) == 0) {
        if(nrow(corPolicyWide) == 0) {break}
        else {for (i in 1:nrow(corPolicyWide)) {domo$pdp_create(ds = dsID, policy_def = createPdpList(corPolicyWide[i,]))}} }
      else{
        # Add policies
        addList <- dplyr::anti_join(corPolicyWide, curPolicyWide, by = c('Policy Name'='Policy Name', 'Policy Column' = 'Policy Column'))
        if(nrow(addList) > 0) {for (i in 1:nrow(addList)) {domo$pdp_create(ds = dsID, policy_def = createPdpList(addList[i,]))} }

        # Delete Policies
        delList <- dplyr::anti_join(curPolicyWide, corPolicyWide, by = c('Policy Name'='Policy Name', 'Policy Column' = 'Policy Column'))
        if(nrow(delList) > 0) {for (i in 1:nrow(delList)) {domo$pdp_delete(ds = dsID, policy = delList$`Policy ID`[i])} }

        # Update Policies
        updList <- curPolicyLong %>%
          dplyr::anti_join(delList %>% dplyr::select(`Policy ID`)) %>%
          dplyr::full_join(corPolicyLong) %>%
          dplyr::filter(!is.na(`Policy ID`)) %>%
          dplyr::filter(is.na(cur) | is.na(cor)) %>%
          dplyr::select(`Policy ID`, `Policy Name`, `Policy Column`) %>%
          unique()
        if(nrow(updList) > 0) {updList <- updList %>%  dplyr::left_join(corPolicyWide)}
        if(nrow(updList) > 0) {for (i in 1:nrow(updList)) {domo$pdp_update(ds = dsID, policy = updList$`Policy ID`[i], policy_def = createPdpList(updList[i,]))} } }
      }

    parallel::stopCluster(cl = my.cluster)
    }
  else { for(b in 1:nrow(pdpDs)){
    dsID <- pdpDs$`Dataset ID`[b]

    # Current PDP List
    curPolicy <- domo$pdp_list(ds = dsID)
    curPolicyLong <- extractPdp(curPolicy) %>% dplyr::filter(`Policy Name` %!like% 'AA - Restricted' & `Policy Name` != 'All Rows') %>% dplyr::mutate(cur = 1)
    curPolicyWide <- curPolicyLong %>% dplyr::arrange(`Policy Name`, `User ID`, `Policy Value`) %>% dplyr::group_by(`Policy ID`, `Policy Name`, `Policy Column`) %>% dplyr::summarise(`User ID` = paste(unique(`User ID`), collapse = '|'), `Policy Value` = paste(unique(`Policy Value`), collapse = '|'))

    # Correct PDP List
    corPolicyLong <- pdpData %>% dplyr::filter(`Dataset ID` == dsID) %>% select(-`Dataset ID`) %>% dplyr::mutate(`User ID` = as.character(`User ID`), `Policy Value` = as.character(`Policy Value`)) %>% dplyr::left_join(curPolicyWide %>% dplyr::select(`Policy ID`, `Policy Name`) %>% unique()) %>% dplyr::mutate(cor = 1)
    corPolicyWide <- corPolicyLong %>% dplyr::arrange(`Policy Name`, `User ID`, `Policy Value`) %>% dplyr::group_by(`Policy Name`, `Policy Column`) %>% dplyr::summarise(`User ID` = paste(unique(`User ID`), collapse = '|'), `Policy Value` = paste(unique(`Policy Value`), collapse = '|'))

    # IF NO current policies can be found on the dataset
    if(nrow(curPolicyWide) == 0) {
      if(nrow(corPolicyWide) == 0) {break}
      else {for (i in 1:nrow(corPolicyWide)) {domo$pdp_create(ds = dsID, policy_def = createPdpList(corPolicyWide[i,]))} } }
    else{
      # Add policies
      addList <- dplyr::anti_join(corPolicyWide, curPolicyWide, by = c('Policy Name'='Policy Name', 'Policy Column' = 'Policy Column'))
      if(nrow(addList) > 0) {for (i in 1:nrow(addList)) {domo$pdp_create(ds = dsID, policy_def = createPdpList(addList[i,]))} }

      # Delete Policies
      delList <- dplyr::anti_join(curPolicyWide, corPolicyWide, by = c('Policy Name'='Policy Name', 'Policy Column' = 'Policy Column'))
      if(nrow(delList) > 0) {for (i in 1:nrow(delList)) {domo$pdp_delete(ds = dsID, policy = delList$`Policy ID`[i])} }

      # Update Policies
      updList <- curPolicyLong %>%
        dplyr::anti_join(delList %>% dplyr::select(`Policy ID`)) %>%
        dplyr::full_join(corPolicyLong) %>%
        dplyr::filter(!is.na(`Policy ID`)) %>%
        dplyr::filter(is.na(cur) | is.na(cor)) %>%
        dplyr::select(`Policy ID`, `Policy Name`, `Policy Column`) %>%
        unique()
        if(nrow(updList) > 0) {updList <- updList %>%  dplyr::left_join(corPolicyWide)}
        if(nrow(updList) > 0) {for (i in 1:nrow(updList)) {domo$pdp_update(ds = dsID, policy = updList$`Policy ID`[i], policy_def = createPdpList(updList[i,]))} } }
  }
  }
}

