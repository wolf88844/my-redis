
use std::{collections::HashMap, sync::{Arc, Mutex}};

use bytes::Bytes;
use mini_redis::{Command, Connection, Frame};
use tokio::net::{TcpListener, TcpStream};


type DB = Arc<Mutex<HashMap<String,Bytes>>>;

#[tokio::main]
async fn main(){
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("listening");
    let db = Arc::new(Mutex::new(HashMap::new()));
    loop{
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        println!("accepted");
        tokio::spawn(async move{
            process(socket,db).await;
        });
    }
}

async fn process(socket: TcpStream,db:DB){

    let mut connection = Connection::new(socket);

    while let Some(frame)=connection.read_frame().await.unwrap(){
        let response = match Command::from_frame(frame).unwrap(){
            Command::Set(cmd)=>{
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(),cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Command::Get(cmd)=>{
                let db = db.lock().unwrap();
                if let Some(value)=db.get(cmd.key()){
                    Frame::Bulk(value.clone().into())
                }else{
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented:{:?}",cmd),
        };
        connection.write_frame(&response).await.unwrap();
    }
}