#' Retrieve All Contributors to Projects & Tasks
#'
#' This function allows you to easily retrieve users contributed to projects or tasks.
#' @param client_id A client_id that can be created on the developer.domo.com page.
#' @param secret A secret that can created on the developer.domo.com page.
#' @examples taskpRo(client_id = client_id,
#'   secret = secret)
#' @export

contributorpRo <- function(client_id, secret){
  domo <- pRoDomo::Domo(client_id = client_id, secret = secret)

  # All Projects
  projects <- domo$projects_all(df_output = FALSE)

  p <- list()
  for (i in 1:length(projects)) {
    p[[i]] <- dplyr::tibble(projectId = projects[[i]]$id,projectOwnedBy = projects[[i]]$createdBy, projectName = projects[[i]]$name, user = projects[[i]]$members %>% unlist())
  }
  p <- bind_rows(p) %>% dplyr::mutate(projectMember = user)


  # All Tasks
  tasks <- list()
  for (i in 1:length(projects)) {tasks[[i]] <- domo$task_list(project_id = projects[[i]]$id)}
  tasks <- dplyr::bind_rows(tasks)

  t <- list()
  for (i in 1:nrow(tasks)) {
    t[[i]] <- merge(tibble(projectId = tasks$projectId[i], listId = tasks$projectListId[i], taskId = tasks$id[i], taskName = tasks$taskName[i], taskOwnedBy = tasks$createdBy[i]),
                    tibble(user = unlist(tasks$contributors[i])))
  }
  t <- dplyr::bind_rows(t) %>% dplyr::mutate(taskContributor = user) %>% dplyr::left_join(p %>% dplyr::select(projectId, projectOwnedBy, projectName) %>% unique())

  # Merge
  out <- dplyr::full_join(p, t) %>%
    dplyr::select(projectId, projectName, projectOwnedBy, projectMember, listId, taskId, taskName, taskOwnedBy, taskContributor)


  return(out)
}
