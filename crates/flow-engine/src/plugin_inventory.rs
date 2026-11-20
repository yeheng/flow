//! Managed worker plugin inventory carried in existing v8 capability vectors.

/// Optional v8 advertisement. An absent inventory denotes an older peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PluginInventory {
    entries: std::collections::BTreeMap<String, (String, String)>,
}

impl PluginInventory {
    pub const MAX_ENTRIES: usize = 256;
    const MARKER: &'static str = "plugin-inventory-v1";
    const PREFIX: &'static str = "plugin-node-v1:";

    pub fn insert(&mut self, node: &str, id: &str, hash: &str) -> Result<(), &'static str> {
        let name = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        };
        if !name(node)
            || !name(id)
            || hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid plugin inventory identity");
        }
        if self.entries.len() >= Self::MAX_ENTRIES || self.entries.contains_key(node) {
            return Err("plugin inventory exceeds its limit or repeats a node type");
        }
        self.entries.insert(node.into(), (id.into(), hash.into()));
        Ok(())
    }

    pub fn from_capabilities(capabilities: &[String]) -> Result<Option<Self>, &'static str> {
        let has_marker = capabilities.iter().any(|value| value == Self::MARKER);
        let mut inventory = Self::default();
        for entry in capabilities
            .iter()
            .filter_map(|value| value.strip_prefix(Self::PREFIX))
        {
            let mut parts = entry.splitn(3, ':');
            let (Some(node), Some(id), Some(hash)) = (parts.next(), parts.next(), parts.next())
            else {
                return Err("malformed plugin inventory entry");
            };
            if !has_marker {
                return Err("plugin inventory marker missing");
            }
            inventory.insert(node, id, hash)?;
        }
        Ok(has_marker.then_some(inventory))
    }

    pub fn capabilities(&self) -> impl Iterator<Item = String> + '_ {
        std::iter::once(Self::MARKER.to_owned()).chain(
            self.entries
                .iter()
                .map(|(node, (id, hash))| format!("{}{node}:{id}:{hash}", Self::PREFIX)),
        )
    }

    pub fn matches(&self, node: &str, id: &str, hash: &str) -> bool {
        self.entries
            .get(node)
            .is_some_and(|(actual_id, actual_hash)| actual_id == id && actual_hash == hash)
    }
}

#[cfg(test)]
mod plugin_inventory_tests {
    use super::PluginInventory;

    #[test]
    fn inventory_distinguishes_legacy_empty_and_exact_matches() {
        assert_eq!(PluginInventory::from_capabilities(&[]), Ok(None));
        let empty = PluginInventory::default();
        let caps: Vec<_> = empty.capabilities().collect();
        assert_eq!(PluginInventory::from_capabilities(&caps), Ok(Some(empty)));

        let hash = "3".repeat(64);
        let mut inventory = PluginInventory::default();
        inventory.insert("ocr", "vision", &hash).unwrap();
        assert!(inventory.matches("ocr", "vision", &hash));
        assert!(!inventory.matches("ocr", "vision", &"4".repeat(64)));

        let mut caps: Vec<_> = inventory.capabilities().collect();
        assert_eq!(
            PluginInventory::from_capabilities(&caps),
            Ok(Some(inventory))
        );
        assert!(PluginInventory::from_capabilities(&caps[1..]).is_err());
        caps.push(caps[1].clone());
        assert!(PluginInventory::from_capabilities(&caps).is_err());
    }
}
