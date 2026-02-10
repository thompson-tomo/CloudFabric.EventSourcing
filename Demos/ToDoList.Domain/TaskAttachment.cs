namespace ToDoList.Domain;

public class TaskAttachment
{
    public TaskAttachment(
        Guid id,
        string originalFileName,
        string storedFilePath,
        string thumbnailFilePath,
        long sizeBytes,
        string contentType
    )
    {
        Id = id;
        OriginalFileName = originalFileName;
        StoredFilePath = storedFilePath;
        ThumbnailFilePath = thumbnailFilePath;
        SizeBytes = sizeBytes;
        ContentType = contentType;
    }

    public Guid Id { get; }

    public string OriginalFileName { get; private set; }

    public string StoredFilePath { get; private set; }

    public string ThumbnailFilePath { get; private set; }

    public long SizeBytes { get; private set; }

    public string ContentType { get; private set; }

    public void UpdateMetadata(string originalFileName, string storedFilePath, string thumbnailFilePath, long sizeBytes, string contentType)
    {
        OriginalFileName = originalFileName;
        StoredFilePath = storedFilePath;
        ThumbnailFilePath = thumbnailFilePath;
        SizeBytes = sizeBytes;
        ContentType = contentType;
    }
}