package redimo

// dedupStrings returns in with duplicates removed, preserving first-seen order. It does
// not mutate in (a set write must not reference the same primary key twice in one
// BatchWriteItem, and Redis counts each distinct member once).
func dedupStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}

	seen := make(map[string]struct{}, len(in))
	out := in[:0:0]

	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}

	return out
}
