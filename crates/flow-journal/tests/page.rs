use flow_journal::{Journal,JournalOptions,Event,EventKind,QueueClass};
use flow_journal::page::{Cursor,Filter,page};
use serde_json::json;

#[tokio::test]
async fn cursor_binds_snapshot_and_empty_pages_advance() {
    let dir=tempfile::tempdir().unwrap();let journal=Journal::open(dir.path(),JournalOptions{group_wait:std::time::Duration::ZERO,..Default::default()}).await.unwrap();
    for i in 0..1030 {let mut event=Event::new(EventKind::Command,json!({}));if i>=1025{event.run_id=Some("r".into());event.run_seq=i-1024;}
        journal.submit(i.to_string(),vec![event],QueueClass::Control).await.unwrap();}
    let upper=journal.durable_lsn();let cursor=Cursor::first(journal.id().into(),"r".into(),Filter::Events,upper);
    let first=page(dir.path(),journal.id(),"r",Filter::Events,cursor,journal.durable_lsn(),2).unwrap();
    assert!(first.events.is_empty());let next=first.next_cursor.unwrap();assert!(next.next_lsn>1);
    let mut wrong=next.clone();wrong.run_id="other".into();assert!(page(dir.path(),journal.id(),"r",Filter::Events,wrong,upper,2).is_err());
    let mut extra=Event::new(EventKind::Command,json!({}));extra.run_id=Some("r".into());extra.run_seq=99;
    journal.submit("later".into(),vec![extra],QueueClass::Control).await.unwrap();
    let mut next=Some(next);let mut seqs=vec![];
    while let Some(cursor)=next{let p=page(dir.path(),journal.id(),"r",Filter::Events,cursor,journal.durable_lsn(),2).unwrap();seqs.extend(p.events.into_iter().map(|e|e.event.run_seq));next=p.next_cursor;}
    assert_eq!(seqs,vec![1,2,3,4,5]);journal.close().await.unwrap();
}
