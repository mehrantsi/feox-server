use super::ConnectionId;
use std::collections::HashMap;

pub struct PatternTrie {
    root: TrieNode,
}

impl Default for PatternTrie {
    fn default() -> Self {
        Self::new()
    }
}

struct TrieNode {
    children: HashMap<u8, Box<TrieNode>>,
    wildcard_child: Option<Box<TrieNode>>,
    single_char_child: Option<Box<TrieNode>>,
    char_class_child: Option<Box<CharClassNode>>,
    subscribers: Vec<(Vec<u8>, ConnectionId)>,
}

struct CharClassNode {
    chars: Vec<u8>,
    negated: bool,
    next: Box<TrieNode>,
}

impl PatternTrie {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    pub fn insert(&mut self, pattern: &[u8], conn_id: ConnectionId) {
        let mut node = &mut self.root;
        let mut i = 0;

        while i < pattern.len() {
            match pattern[i] {
                b'*' => {
                    if node.wildcard_child.is_none() {
                        node.wildcard_child = Some(Box::new(TrieNode::new()));
                    }
                    node = node.wildcard_child.as_mut().unwrap();
                    i += 1;
                }
                b'?' => {
                    if node.single_char_child.is_none() {
                        node.single_char_child = Some(Box::new(TrieNode::new()));
                    }
                    node = node.single_char_child.as_mut().unwrap();
                    i += 1;
                }
                b'[' => {
                    let (char_class, next_i) = parse_char_class(&pattern[i..]);
                    if let Some((chars, negated)) = char_class {
                        if node.char_class_child.is_none() {
                            node.char_class_child = Some(Box::new(CharClassNode {
                                chars,
                                negated,
                                next: Box::new(TrieNode::new()),
                            }));
                        }
                        node = &mut node.char_class_child.as_mut().unwrap().next;
                        i += next_i;
                    } else {
                        node = node
                            .children
                            .entry(pattern[i])
                            .or_insert_with(|| Box::new(TrieNode::new()));
                        i += 1;
                    }
                }
                b'\\' if i + 1 < pattern.len() => {
                    i += 1;
                    node = node
                        .children
                        .entry(pattern[i])
                        .or_insert_with(|| Box::new(TrieNode::new()));
                    i += 1;
                }
                c => {
                    node = node
                        .children
                        .entry(c)
                        .or_insert_with(|| Box::new(TrieNode::new()));
                    i += 1;
                }
            }
        }

        node.subscribers.push((pattern.to_vec(), conn_id));
    }

    pub fn remove(&mut self, pattern: &[u8], conn_id: ConnectionId) -> bool {
        Self::remove_from_node(&mut self.root, pattern, 0, pattern, conn_id)
    }

    fn remove_from_node(
        node: &mut TrieNode,
        pattern: &[u8],
        pos: usize,
        full_pattern: &[u8],
        conn_id: ConnectionId,
    ) -> bool {
        if pos == pattern.len() {
            let before_len = node.subscribers.len();
            node.subscribers
                .retain(|(p, id)| !(p == full_pattern && *id == conn_id));
            return node.subscribers.len() < before_len;
        }

        match pattern[pos] {
            b'*' => {
                if let Some(ref mut child) = node.wildcard_child {
                    return Self::remove_from_node(child, pattern, pos + 1, full_pattern, conn_id);
                }
            }
            b'?' => {
                if let Some(ref mut child) = node.single_char_child {
                    return Self::remove_from_node(child, pattern, pos + 1, full_pattern, conn_id);
                }
            }
            b'[' => {
                let (char_class, next_i) = parse_char_class(&pattern[pos..]);
                if char_class.is_some() {
                    if let Some(ref mut child) = node.char_class_child {
                        return Self::remove_from_node(
                            &mut child.next,
                            pattern,
                            pos + next_i,
                            full_pattern,
                            conn_id,
                        );
                    }
                } else if let Some(ref mut child) = node.children.get_mut(&pattern[pos]) {
                    return Self::remove_from_node(child, pattern, pos + 1, full_pattern, conn_id);
                }
            }
            b'\\' if pos + 1 < pattern.len() => {
                if let Some(ref mut child) = node.children.get_mut(&pattern[pos + 1]) {
                    return Self::remove_from_node(child, pattern, pos + 2, full_pattern, conn_id);
                }
            }
            c => {
                if let Some(ref mut child) = node.children.get_mut(&c) {
                    return Self::remove_from_node(child, pattern, pos + 1, full_pattern, conn_id);
                }
            }
        }

        false
    }

    pub fn find_matches(&self, channel: &[u8]) -> Vec<(Vec<u8>, ConnectionId)> {
        let mut matches = Vec::new();
        self.find_in_node(&self.root, channel, 0, &mut matches);
        matches
    }

    fn find_in_node(
        &self,
        node: &TrieNode,
        channel: &[u8],
        pos: usize,
        matches: &mut Vec<(Vec<u8>, ConnectionId)>,
    ) {
        if pos == channel.len() {
            matches.extend(node.subscribers.clone());

            if let Some(ref wildcard) = node.wildcard_child {
                matches.extend(wildcard.subscribers.clone());
            }
            return;
        }

        let current_char = channel[pos];

        if let Some(child) = node.children.get(&current_char) {
            self.find_in_node(child, channel, pos + 1, matches);
        }

        if let Some(ref single) = node.single_char_child {
            self.find_in_node(single, channel, pos + 1, matches);
        }

        if let Some(ref char_class) = node.char_class_child {
            if self.matches_char_class(current_char, &char_class.chars, char_class.negated) {
                self.find_in_node(&char_class.next, channel, pos + 1, matches);
            }
        }

        if let Some(ref wildcard) = node.wildcard_child {
            matches.extend(wildcard.subscribers.clone());

            for i in pos..=channel.len() {
                self.find_in_node(wildcard, channel, i, matches);
            }
        }
    }

    fn matches_char_class(&self, c: u8, chars: &[u8], negated: bool) -> bool {
        let matches = chars.contains(&c);
        if negated {
            !matches
        } else {
            matches
        }
    }

    pub fn clear(&mut self) {
        self.root = TrieNode::new();
    }

    pub fn get_all_patterns(&self) -> Vec<Vec<u8>> {
        let mut patterns = Vec::new();
        Self::collect_patterns(&self.root, &mut patterns);
        patterns
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn collect_patterns(node: &TrieNode, patterns: &mut Vec<Vec<u8>>) {
        for (pattern, _) in &node.subscribers {
            patterns.push(pattern.clone());
        }

        for child in node.children.values() {
            Self::collect_patterns(child, patterns);
        }

        if let Some(ref wildcard) = node.wildcard_child {
            Self::collect_patterns(wildcard, patterns);
        }

        if let Some(ref single) = node.single_char_child {
            Self::collect_patterns(single, patterns);
        }

        if let Some(ref char_class) = node.char_class_child {
            Self::collect_patterns(&char_class.next, patterns);
        }
    }
}

fn parse_char_class(pattern: &[u8]) -> (Option<(Vec<u8>, bool)>, usize) {
    if pattern.is_empty() || pattern[0] != b'[' {
        return (None, 0);
    }

    let mut i = 1;
    let negated = pattern.get(1) == Some(&b'^');
    if negated {
        i = 2;
    }

    let mut chars = Vec::new();
    let mut escaped = false;

    while i < pattern.len() {
        if escaped {
            chars.push(pattern[i]);
            escaped = false;
        } else if pattern[i] == b'\\' {
            escaped = true;
        } else if pattern[i] == b']' {
            return (Some((chars, negated)), i + 1);
        } else if pattern[i] == b'-'
            && !chars.is_empty()
            && i + 1 < pattern.len()
            && pattern[i + 1] != b']'
        {
            let start = *chars.last().unwrap();
            let end = pattern[i + 1];
            for c in (start + 1)..=end {
                chars.push(c);
            }
            i += 1;
        } else {
            chars.push(pattern[i]);
        }
        i += 1;
    }

    (None, 0)
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            wildcard_child: None,
            single_char_child: None,
            char_class_child: None,
            subscribers: Vec::new(),
        }
    }
}
