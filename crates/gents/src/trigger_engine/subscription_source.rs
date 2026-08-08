//! Subscription factory for the three event-driven sources.

use std::sync::Arc;
use std::time::Duration;

use defra_node::EmbeddedNode;
use events::{EventName, Subscription};

pub trait UpdateSubscriptionSource: Send + Sync {
    fn subscribe_updates(&self) -> Subscription;
}

/// Prevent a permanently closed event bus from turning subscription recovery
/// into a hot loop while keeping durable rescans responsive.
pub(crate) const UPDATE_SUBSCRIPTION_REOPEN_DELAY: Duration = Duration::from_millis(100);

impl UpdateSubscriptionSource for EmbeddedNode {
    fn subscribe_updates(&self) -> Subscription {
        self.subscribe(&[EventName::Update])
    }
}

impl UpdateSubscriptionSource for Arc<EmbeddedNode> {
    fn subscribe_updates(&self) -> Subscription {
        self.as_ref().subscribe(&[EventName::Update])
    }
}
