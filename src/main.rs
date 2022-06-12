use dotenv;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::collections::HashMap;
use std::env;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Event {
    url: String,
    visitor_id: String,
    timestamp: Number,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Session {
    duration: Number,
    pages: Vec<String>,
    start_time: Number,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let post_uri = env::var("POST_API_URI").unwrap();
    let get_uri = env::var("GET_API_URI").unwrap();

    let resp = reqwest::get(get_uri).await?.text().await?;

    let json: HashMap<String, Vec<Event>> = serde_json::from_str(&resp).expect("Error");
    let mut result: HashMap<String, Vec<Session>> = HashMap::new();
    let mut events_by_user: HashMap<String, Vec<Event>> = HashMap::new();

    for event in &json["events"] {
        if events_by_user.contains_key(&event.visitor_id) {
            events_by_user
                .get_mut(&event.visitor_id)
                .unwrap()
                .push(event.clone());
        } else {
            events_by_user.insert(event.visitor_id.clone(), vec![event.clone()]);
        }
    }

    for (user, mut events) in events_by_user.clone() {
        events.sort_by(|a, b| {
            a.timestamp
                .as_u64()
                .unwrap()
                .cmp(&b.timestamp.as_u64().unwrap())
        });

        events_by_user.insert(user, events);
    }

    for (visitor_id, events) in events_by_user {
        for event in &events {
            let mut did_add_to_session = false;

            if result.contains_key(&visitor_id) {
                let mut sessions = result[&visitor_id][..].to_vec();

                for mut session in sessions[..].to_vec() {
                    let latest_occurence =
                        session.start_time.as_u64().unwrap() + session.duration.as_u64().unwrap();

                    if event.timestamp.as_u64().unwrap() - latest_occurence <= 600000 {
                        session.pages.push(event.url.clone());
                        session.duration = serde_json::Number::from(
                            event.timestamp.as_u64().unwrap()
                                - session.start_time.as_u64().unwrap(),
                        );
                        sessions.push(session.clone());
                        result.insert(visitor_id.clone(), sessions.clone());
                        did_add_to_session = true;
                        continue;
                    }
                }

                if !did_add_to_session {
                    let duration: i8 = 0;
                    let new_session: Session = Session {
                        duration: serde_json::Number::from(duration),
                        pages: vec![event.url.clone()],
                        start_time: event.timestamp.clone(),
                    };
                    sessions.push(new_session);
                    result.insert(visitor_id.clone(), sessions);
                }
            } else {
                let duration: i8 = 0;
                let new_session: Session = Session {
                    duration: serde_json::Number::from(duration),
                    pages: vec![event.url.clone()],
                    start_time: event.timestamp.clone(),
                };
                result.insert(visitor_id.clone(), vec![new_session]);
            }
        }
    }

    let client = reqwest::Client::new();
    let mut return_val = HashMap::new();
    return_val.insert("sessionByUser", &result);
    let res = client.post(post_uri).json(&return_val).send().await?;

    println!("{:?}", res.status());
    Ok(())
}
