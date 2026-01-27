QUERY createFile(name: String, extension: String, text: String) => 
    file <- AddN<File>({name:name, extension:extension, text:text})
    RETURN file

QUERY getAllFiles() => 
    files <- N<File>
    RETURN files

// Property Exclusion (empty)
QUERY getAllFiles1() => 
    files <- N<File>
    RETURN files::!{text}

// Spread Operator (can't compile)
QUERY getAllFiles2(id: ID) => 
    files <- N<File>(id)
    RETURN files::{
        file_id: ID,
        name: name,
        extension: extension,
        extracted_at: extracted_at,
        other: _::Out<FileEdge>
    }

// Accessing ID (empty)
QUERY getAllFileIds() => 
    files <- N<File>
    RETURN files::ID

QUERY getFileText(file_id: ID) => 
    file <- N<File>(file_id)
    RETURN file::{text}

QUERY getFileMult(file_id: ID) => 
    file <- N<File>(file_id)
    RETURN file::{text, name}

QUERY getFileText1(file_id: ID) => 
    file <- N<File>
    RETURN file::{text, name}