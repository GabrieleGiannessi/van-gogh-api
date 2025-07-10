class DocumentCreationError(Exception):
    pass


class DocumentIndexingError(Exception):
    pass


class PDFExtractionError(Exception):
    pass

class StreamError(Exception): 
    pass

class DocumentDeleteError(Exception):
    pass

class DocumentNotFound(Exception):
    pass

class PreviewException(Exception):
    pass