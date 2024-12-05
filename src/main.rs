extern crate rusqlite;
extern crate regex;

use regex::Regex;
use rusqlite::{params, Connection, Result as RusqliteResult};
use std::fs::{File, read_dir};
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

        CREATE TABLE IF NOT EXISTS words (
            position INTEGER,
            word TEXT,
            line INTEGER,
            page INTEGER,
            file_name TEXT,
            PRIMARY KEY(position, file_name)
            FOREIGN KEY(file_name) REFERENCES files(name)
        );
        ",
    )?;

    Ok(())
}

fn load_file(name: &str, connection: &mut Connection) -> RusqliteResult<(), Error> {
    let file = File::open(name)?;
    let reader = BufReader::new(file);

    let tx = connection.transaction()?;

    tx.execute(
        "INSERT INTO files (name) VALUES (?1)",
        params![name.replace(".txt", "").replace("./Texts/", "")],
    )?;
    
    let regex = Regex::new(r"[^\p{L}\p{N}'’]+").unwrap();
    let mut line_index = 1;
    let mut word_index = 1;

    for line_result in reader.lines() {

        let line_content = regex.replace_all(&line_result?, " ").trim().to_string().to_lowercase();
                
        let words: Vec<&str> = line_content.split_whitespace().collect();
        let page = (line_index as f32 / 46.0).floor() as i32 + 1;

        if words != [""] {
            for word in words {
                
                tx.execute(
                    "INSERT INTO words (position, word, line, page, file_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![word_index, word, line_index, page, name.replace(".txt", "").replace("./Texts/", "")],
                )?;
                
                word_index += 1;
            }
        }
        line_index += 1;
    }

    tx.commit()?;

    Ok(())
}

fn find_contexts(target_words: Vec<&str>, name: &str, connection: &Connection) -> RusqliteResult<Vec<String>, Error> {
    let mut contexts: Vec<String> = vec![];
    let mut case_words: Vec<String> = target_words.iter().map(|word| word.to_lowercase()).collect();
    case_words.sort();

    for search in case_words {

        let mut prepare = connection.prepare("SELECT position, page FROM words WHERE word = (?1) AND file_name = (?2)")?;
        let mut query_result = prepare.query(params![search, name])?;

        while let Some(word) = query_result.next()? {

            let position: i32 = word.get(0)?;
            let page: i32 = word.get(1)?;
            let mut context: Vec<String> = vec![];

            let mut prepare = connection.prepare("SELECT word FROM words WHERE position in (?1, ?2, ?3, ?4, ?5) AND file_name = (?6)")?;
            let mut query_result = prepare.query(params![position - 2, position - 1, position, position + 1, position + 2, name])?;

            while let Some(word) = query_result.next()? {
                let word_content: String = word.get(0)?;
                context.push(word_content);
            }

            context.push(format!(": {}", page));
            contexts.push(context.join(" "));
        }
    }

    return Ok(contexts)
}

fn get_texts() -> StdResult<Vec<String>> {
    let mut file_names = Vec::new();

    for entry in read_dir("./Texts")? {
        let path = entry?.path();
        if let Some(file_name) = path.file_name() {
            if let Some(file_str) = file_name.to_str() {
                file_names.push("./Texts/".to_owned() + file_str);
            }
        }
    }
    Ok(file_names)
}

fn main() -> RusqliteResult<(), Error>{
    let texts = get_texts()?;

    let target_file = String::from("Dom Casmurro");
    let target_words = vec!["Dizer"];
    let connection = Connection::open("Words in context.db")?;

    create_tables(&connection)?;

    for file_name in texts {
        let mut prepare = connection.prepare("SELECT * FROM files WHERE name = ?1")?;
        let mut query_result = prepare.query(params![file_name.replace(".txt", "").replace("./Texts/", "")])?;
        
        let mut connection = Connection::open("Words in context.db")?;

        if let None = query_result.next()? {
            load_file(&file_name, &mut connection)?
        }
    }

    for context in find_contexts(target_words, &target_file, &connection)? {
        println!("{}", context);
    }

    Ok(())
}