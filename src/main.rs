extern crate rusqlite;
extern crate regex;

use rusqlite::{params, Connection, Result as RusqliteResult};
use std::fs::{read_dir, File};
use std::path::Path;
use std::io::{self, stdin, BufRead, BufReader, Result as StdResult};

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

fn get_dir() -> RusqliteResult<Vec<String>, Error> {
    let path = Path::new("./Texts");
    let contents = read_dir(path)?;

    let files = contents.filter_map(
        |content| {
            content.ok().and_then(
                |entry| {
                    if entry .file_type().map(|kind| kind.is_file()).unwrap_or(false) {
                        let temp = entry.file_name();
                        let name = temp.to_string_lossy();
                        if name.ends_with(".txt") {
                            Some(name.into_owned())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            )
        }
    ).collect();

    Ok(files)
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
            word TEXT,
            content TEXT,
            line INTEGER,
            file_name TEXT,
            PRIMARY KEY (position, line, file_name)
            FOREIGN KEY (line, file_name) references lines (position, file_name)
        );
        "
    )?;

    Ok(())
}

fn load_file(name: &str, connection: &mut Connection) -> RusqliteResult<(), Error> {
    let file = File::open("./Texts/".to_owned() + name)?;
    let reader = BufReader::new(file);
    let file_name = name.replace(".txt", "");

    let tx = connection.transaction()?;

    tx.execute(
        "INSERT INTO files (name) VALUES (?1)",
        params![file_name],
    )?;

    let mut line_index = 1;

    for line_result in reader.lines() {

        let line_content: String = line_result?.chars().filter(|c| c.is_alphanumeric() || c == &' ').collect();

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
                    "INSERT INTO contexts (position, word, content, line, file_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![context_index, word.to_lowercase(), context, line_index, file_name],
                )?;

                context_index += 1;
            }
        }
        line_index += 1;
    }

    tx.commit()?;

    Ok(())
}

fn get_contexts(words: Vec<&str>, files: Vec<&str>, connection: &mut Connection) -> RusqliteResult<Vec<(String, String)>> {
    let mut contexts: Vec<(String, String)> = vec![];

    let temp_file = files.clone();
    let files_slice = temp_file.as_slice();
    let words_slice = words.as_slice();

    let sql = match (words_slice, files_slice) {
        ([""], [""]) => "
        SELECT lines.content, contexts.content
        FROM contexts, lines
        WHERE contexts.line = lines.position
        AND contexts.file_name = lines.file_name",
        (_, [""]) => "
        SELECT lines.content, contexts.content
        FROM contexts, lines
        WHERE contexts.line = lines.position
        AND contexts.file_name = lines.file_name
        AND contexts.word = (?1)",
        ([""], _) => "
        SELECT lines.content, contexts.content
        FROM contexts, lines
        WHERE contexts.line = lines.position
        AND contexts.file_name = lines.file_name
        AND contexts.file_name = (?1)
        AND lines.file_name = (?1)",
        (_, _) => "
        SELECT lines.content, contexts.content
        FROM contexts, lines
        WHERE contexts.line = lines.position
        AND contexts.file_name = (?1)
        AND lines.file_name = (?1)
        AND contexts.word = (?2)",
    };

    for file in files {
        for word in &words {
            let mut prepare = connection.prepare(sql)?;

            let mut query_result = match (files_slice, words_slice) {
                ([""], [""]) => prepare.query(params![])?,
                (_, [""]) => prepare.query(params![file.trim()])?,
                ([""], _) => prepare.query(params![word.trim()])?,
                (_, _) => prepare.query(params![file.trim(), word.trim()])?
            };

            while let Some(row) = query_result.next()? {
                contexts.push((row.get(0)?, row.get(1)?));
            }
        }
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
    let mut connection = Connection::open("Words in context.db")?;

    create_tables(&connection)?;

    for file in get_dir()? {
        let mut prepare = connection.prepare("SELECT name FROM files where name = (?1)")?;
        let mut query_result = prepare.query(params![file.replace(".txt", "").replace("./Texts/", "")])?;
        
        let mut connection = Connection::open("Words in context.db")?;

        if let None = query_result.next()? {
            load_file(&file, &mut connection)?
        }
    }

    let mut input_words = String::new();
    let mut input_files = String::new();

    println!("Words to search (separated by ','):");
    stdin().read_line(&mut input_words).expect("Error reading input");
    let input_words = input_words.trim();
    let words: Vec<&str> = input_words.split(",").collect();

    println!("Files to search (separated by ','):");
    stdin().read_line(&mut input_files).expect("Error reading input");
    let input_files = input_files.trim();
    let files: Vec<&str> = input_files.split(",").collect();

    print!("\n");

    for (line, context) in get_contexts(words, files, &mut connection)? {
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
    fn test_get_dir() {
        File::create("./Texts/zzz1.txt").expect("Error creating file");
        File::create("./Texts/zzz2.txt").expect("Error creating file");

        let dir = get_dir().expect("Error getting directory");

        assert_eq!(dir[dir.len() - 2..], vec!["zzz1.txt", "zzz2.txt"]);

        remove_file("./Texts/zzz1.txt").expect("Error removing file");
        remove_file("./Texts/zzz2.txt").expect("Error removing file");
    }

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

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
    }

    #[test]
    fn test_load_file() {
        let mut connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");

        let mut file = File::create("./Texts/test.txt").expect("Error creating file");
        write!(file, "The quick brown fox\nA brown cat sat\nThe cat is brown").expect("Error writing to file");

        load_file("test.txt", &mut connection).expect("Error loading file");

        let query: Option<String> = connection.query_row(
            "SELECT name FROM files WHERE name = ?1",
            ["test"],
            |row| row.get(0),
        ).expect("Error querying connection");

        assert_eq!(query, Some("test".to_string()));

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
        remove_file("./Texts/test.txt").expect("Error removing file");
    }

    #[test]
    fn test_check_contexts() {
        let mut connection = Connection::open("test.db").expect("Error getting connection");

        create_tables(&connection).expect("Error creating tables");

        let mut file = File::create("./Texts/test.txt").expect("Error creating file");
        write!(file, "The quick brown fox").expect("Error writing to file");

        load_file("test.txt", &mut connection).expect("Error loading file");

        let contexts = get_contexts(vec!["quick", " brown", " fox"], vec!["test"], &mut connection).expect("Error getting contexts");

        assert_eq!(contexts, vec![("The quick brown fox".to_string(), "brown fox The quick".to_string()),
                                  ("The quick brown fox".to_string(), "fox The quick brown".to_string()),
                                  ("The quick brown fox".to_string(), "quick brown fox The".to_string())]);

        connection.close().expect("Error closing connection");

        remove_file("test.db").expect("Error removing file");
        remove_file("./Texts/test.txt").expect("Error removing file");
    }
}
