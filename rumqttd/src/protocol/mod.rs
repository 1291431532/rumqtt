pub mod v4;
//pub mod v5;

/// Checks if a topic or topic filter has wildcards
pub fn has_wildcards(s: &str) -> bool {
    s.contains('+') || s.contains('#')
}

/// Checks if a topic is valid
pub fn valid_topic(topic: &str) -> bool {
    if topic.contains('+') {
        return false;
    }

    if topic.contains('#') {
        return false;
    }

    true
}

/// Checks if the filter is valid
///
/// https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let hirerarchy = filter.split('/').collect::<Vec<&str>>();
    if let Some((last, remaining)) = hirerarchy.split_last() {
        // # is not allowed in filer except as a last entry
        // invalid: sport/tennis#/player
        // invalid: sport/tennis/#/ranking
        for entry in remaining.iter() {
            if entry.contains('#') {
                return false;
            }
        }

        // only single '#" is allowed in last entry
        // invalid: sport/tennis#
        if last.len() != 1 && last.contains('#') {
            return false;
        }
    }

    true
}

/// Checks if topic matches a filter. topic and filter validation isn't done here.
///
/// **NOTE**: 'topic' is a misnomer in the arg. this can also be used to match 2 wild subscriptions
/// **NOTE**: make sure a topic is validated during a publish and filter is validated
/// during a subscribe
pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some(t) if t == "#" => return false,
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}
