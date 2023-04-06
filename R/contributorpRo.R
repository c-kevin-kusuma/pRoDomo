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
    p[[i]] <- dplyr::tibble(projectId = projects[[i]]$id, user = projects[[i]]$members %>% unlist())
  }
  p <- bind_rows(p)


  # All Tasks
  tasks <- list()
  for (i in 1:length(projects)) {tasks[[i]] <- domo$task_list(project_id = projects[[i]]$id)}
  tasks <- dplyr::bind_rows(tasks)

  contributor <- list()
  for (i in 1:nrow(tasks)) {
    a <- domo$task_get(project_id = tasks$projectId[i], list_id = tasks$projectListId[i], task_id = tasks$id[i])
    contributor[[i]] <- dplyr::tibble(projectId = tasks$projectId[i], listId = tasks$projectListId[i], taskId = tasks$id[i], user = a$contributors %>% unlist())
  }
  contributor <- bind_rows(contributor)
  # Merge
  out <- p %>% left_join(contributor)

  return(out)
}
