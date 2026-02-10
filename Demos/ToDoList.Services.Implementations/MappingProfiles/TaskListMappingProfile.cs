using AutoMapper;
using ToDoList.Domain;
using ToDoList.Domain.Projections.TaskLists;
using ToDoList.Models.ViewModels.TaskLists;

namespace ToDoList.Services.Implementations.MappingProfiles;

public class TaskListMappingProfile : Profile
{
    public TaskListMappingProfile()
    {
        CreateMap<TaskListShareProjection, TaskListShareViewModel>();
        CreateMap<TaskListShare, TaskListShareViewModel>();
        CreateMap<TaskListProjectionItem, TaskListViewModel>();
        CreateMap<TaskListProjectionItem, TaskListWithTasksViewModel>();
        CreateMap<TaskList, TaskListViewModel>();
        CreateMap<TaskList, TaskListWithTasksViewModel>();
        
        CreateMap<TaskListViewModel, TaskListWithTasksViewModel>();
    }
}