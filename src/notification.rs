use std::time::Duration;

use crate::mailer::Mailer;

const NOTIFICATION_TICK_INTERVAL: Duration = Duration::from_secs(300);

pub fn get_new_notifications() {}

pub fn start_notification_service(mailer: Mailer) {
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(NOTIFICATION_TICK_INTERVAL);
    loop {
      interval.tick().await;
    }
  });
}
