extern crate rusqlite;
extern crate regex;

use rusqlite::{params, Connection, Result as RusqliteResult};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Result as StdResult};

#[allow(dead_code)]
#[derive(Debug)]
enum Error {
    RusqliteError(rusqlite::Error),
    StdError(io::Error)
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::StdError(error)
    }
}
impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Error::RusqliteError(error)
    }
}

fn create_tables(connection: &Connection) -> RusqliteResult<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS files (
            name TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS lines (
            position INTEGER,
            content TEXT,
            file_name TEXT, 
            PRIMARY KEY (position, file_name),
            FOREIGN KEY (file_name) REFERENCES files (name)
        );

        CREATE TABLE IF NOT EXISTS contexts (
            position INTEGER,
            line INTEGER,
            content TEXT,
            file_name TEXT,
            PRIMARY KEY (position, line, file_name)
            FOREIGN KEY (line, file_name) references lines (position, file_name)
        );
        "
    )?;

    Ok(())
}

fn load_file(name: &str, connection: &mut Connection) -> RusqliteResult<(), Error> {
    let file = File::open(name)?;
    let reader = BufReader::new(file);
    let file_name = name.replace(".txt", "").replace("./Texts/", "");

    let tx = connection.transaction()?;

    tx.execute(
        "INSERT INTO files (name) VALUES (?1)",
        params![file_name],
    )?;

    let mut line_index = 1;

    for line_result in reader.lines() {

        let line_content = line_result?;

        tx.execute(
            "INSERT INTO lines (position, content, file_name) VALUES (?1, ?2, ?3)",
            params![line_index, line_content, file_name],
        )?;
        
        let words = line_content.split_whitespace().map(|word| word.to_string()).collect();
        let formatted_words = remove_stop_words(&words, &get_stop_words()?);

        let size = words.len();

        let mut context_index = 1;

        for word in formatted_words.iter() {

            let positions: Vec<usize> = words.iter().enumerate()
                .filter_map(|(index, value)| if value == word {Some(index)} else {None}).collect();
        
            let clone = words.clone();

            for position in positions {
                let mut context = String::new();

                for i in position..(position + size) {
                    context.push_str(&(clone[i % size].to_owned() + " "));
                }

                context.pop();

                tx.execute(
                    "INSERT INTO contexts (position, line, content, file_name) VALUES (?1, ?2, ?3, ?4)",
                    params![context_index, line_index, context, file_name],
                )?;

                context_index += 1;
            }
        }
        line_index += 1;
    }

    tx.commit()?;

    Ok(())
}

fn get_contexts(name: &str, connection: &mut Connection) -> RusqliteResult<Vec<(String, String)>> {
    let file_name = name.replace(".txt", "").replace("./Texts/", "");
    let mut contexts: Vec<(String, String)> = vec![];
    
    let mut prepare = connection.prepare("
                                                        SELECT lines.content AS line, contexts.content AS context 
                                                        FROM lines 
                                                        LEFT JOIN contexts ON contexts.line = lines.position 
                                                        WHERE lines.file_name = (?1)"
                                                        )?;
    let mut query_result = prepare.query(params![file_name])?;

    while let Some(row) = query_result.next()? {
        contexts.push((row.get(0)?, row.get(1)?));
    }

    contexts.sort_by(|a, b| a.1.to_lowercase().cmp(&b.1.to_lowercase()));

    Ok(contexts)
}

fn get_stop_words() -> StdResult<Vec<String>> {
    let mut stop_words = Vec::new();

    if let Ok(file) = File::open("./Resources/stop_words.txt") {
        for line in BufReader::new(file).lines() {
            if let Ok(word) = line {
                stop_words.push(word);
            }
        }
    }

    Ok(stop_words) 
}

fn remove_stop_words(string: &Vec<String>, stop_words: &Vec<String>) -> Vec<String> {
    let mut formated_string: Vec<String> = vec![];

    for word in string {
        if !stop_words.contains(&word.to_lowercase()) {
            formated_string.push(word.to_string());
        }
    }

    return formated_string
}

fn main() -> RusqliteResult<(), Error>{

    let file = String::from("./Texts/The Quick Brown Fox.txt");
    let connection = Connection::open("Words in context.db")?;

    create_tables(&connection)?;

    let mut prepare = connection.prepare("SELECT name FROM files where name = (?1)")?;
    let mut query_result = prepare.query(params![file.replace(".txt", "").replace("./Texts/", "")])?;
    
    let mut connection = Connection::open("Words in context.db")?;

    if let None = query_result.next()? {
        load_file(&file, &mut connection)?
    }

    for (line, context) in get_contexts(&file, &mut connection)? {
        println!("{} (from '{}') ", context, line);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs::{File, remove_file};
    use std::io::Write;

    #[test]
    fn test_remove_stop_words() {
        let stop_words = vec!["the".to_string(), "a".to_string()];
        let string = vec![
            "The".to_string(),
            "quick".to_string(),
            "brown".to_string(),
            "fox".to_string(),
            "a".to_string(),
            "brown".to_string(),
            "cat".to_string(),
            "sat".to_string()
        ];

        let result = remove_stop_words(&string, &stop_words);

        assert_eq!(result, vec![
            "quick".to_string(),
            "brown".to_string(),
            "fox".to_string(),
            "brown".to_string(),
            "cat".to_string(),
            "sat".to_string(),
        ]);
    }

    #[test]
    fn test_get_stop_words() {
        let stop_words = get_stop_words().expect("Error reading stop words file");

        assert!(stop_words.contains(&"the".to_string()));
        assert!(stop_words.contains(&"who".to_string()));
    }

    #[test]
    fn test_create_tables() {
        let connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");

        let query = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table'",
            [],
            |row| row.get::<usize, i64>(0),
        ).expect("Error querying connection");

        assert_eq!(query, 3);

        remove_file("test.db").expect("Error removing file");
    }

    #[test]
    fn test_load_file() {
        let mut connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");

        let file_name = "test.txt";

        let mut file = File::create(file_name).expect("Error creating file");
        write!(file, "The quick brown fox\nA brown cat sat\nThe cat is brown").expect("Error writing to file");

        load_file(file_name, &mut connection).expect("Error loading file");

        let query: Option<String> = connection.query_row(
            "SELECT name FROM files WHERE name = ?1",
            [file_name.replace("./Texts/", "").replace(".txt", "")],
            |row| row.get(0),
        ).expect("Error querying connection");

        assert_eq!(query, Some(file_name.replace("./Texts/", "").replace(".txt", "")));

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
        remove_file(file_name).expect("Error removing file");
    }

    #[test]
    fn test_get_contexts() {
        let mut connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");
        
        let file_name = "test.txt";

        let mut file = File::create(file_name).expect("Error creating file");
        write!(file, "The quick brown fox").expect("Error writing to file");

        load_file(file_name, &mut connection).expect("Error loading file");

        let contexts = get_contexts(file_name, &mut connection).expect("Error getting contexts");

        assert_eq!(contexts.len(), 3);

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
        remove_file(file_name).expect("Error removing file");
    }

    #[test]
    fn test_check_contexts() {
        let mut connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");
        
        let file_name = "test.txt";

        let mut file = File::create(file_name).expect("Error creating file");
        write!(file, "The quick brown fox").expect("Error writing to file");

        load_file(file_name, &mut connection).expect("Error loading file");

        let contexts = get_contexts(file_name, &mut connection).expect("Error getting contexts");

        assert_eq!(contexts, vec![("The quick brown fox".to_string(), "brown fox The quick".to_string()),
                                  ("The quick brown fox".to_string(), "fox The quick brown".to_string()),
                                  ("The quick brown fox".to_string(), "quick brown fox The".to_string())]);

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
        remove_file(file_name).expect("Error removing file");
    }
}
