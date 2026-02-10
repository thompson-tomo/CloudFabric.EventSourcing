using AutoMapper;
using ToDoList.Domain;
using ToDoList.Domain.Projections.TaskLists;
using ToDoList.Models.ViewModels.TaskLists;

namespace ToDoList.Services.Implementations.MappingProfiles;

public class TaskMappingProfile : Profile
{
    public TaskMappingProfile()
    {
        CreateMap<SubTaskItem, SubTaskViewModel>();
        CreateMap<TaskAttachment, TaskAttachmentViewModel>();
        CreateMap<SubTaskProjectionItem, SubTaskViewModel>();
        CreateMap<TaskAttachmentProjectionItem, TaskAttachmentViewModel>();

        CreateMap<ToDoList.Domain.Task, TaskViewModel>()
            .ForMember(dest => dest.IsClosed, opt => opt.MapFrom(src => src.IsCompleted));

        CreateMap<TaskProjectionItem, TaskViewModel>()
            .ForMember(dest => dest.IsClosed, opt => opt.MapFrom(src => src.IsCompleted));
    }
}
