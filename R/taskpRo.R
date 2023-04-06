#' Retrieve All Tasks From All Projects
#'
#' This function allows you to easily retrieve all active lists & tasks from active projects.
#' @param client_id A client_id that can be created on the developer.domo.com page.
#' @param secret A secret that can created on the developer.domo.com page.
#' @examples taskpRo(client_id = client_id,
#'   secret = secret)
#' @export

taskpRo <- function(client_id, secret){
  domo <- pRoDomo::Domo(client_id = client_id, secret = secret)

  # All Projects
  projects <- domo$projects_all() %>%
    dplyr::rename(projectId = id, projectName = name, projectDescription = description, projectMembers = members,
                  projectCreatedBy = createdBy, projectCreatedDate = createdDate, projectPublic = public, projectDueDate = dueDate)

  # All Lists
  lists <- list()
  for (i in 1:nrow(projects)) {lists[[i]] <- domo$project_list_get(project_id = projects$projectId[i]) %>% dplyr::mutate(projectId = projects$projectId[i])}
  lists <- dplyr::bind_rows(lists) %>%
    dplyr::rename(listId = id, listName = name, listType = type, listIndex = index)

  # All Tasks
  tasks <- list()
  for (i in 1:nrow(projects)) {tasks[[i]] <- domo$task_list(project_id = projects$projectId[i])}
  tasks <- dplyr::bind_rows(tasks) %>%
    dplyr::rename(taskId = id, listId = projectListId, taskDescription = description, taskCreatedDate = createdDate,
                  taskDueDate = dueDate, taskPriority = priority, taskCreatedBy = createdBy, taskOwnedBy = ownedBy,
                  taskContributors = contributors, taskAttachmentCount = attachmentCount, taskTags = tags, taskArchived = archived)

  # Merge
  out <- projects %>% dplyr::inner_join(lists) %>% dplyr::inner_join(tasks)

  return(out)
}
