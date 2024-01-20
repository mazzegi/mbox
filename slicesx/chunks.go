package slicesx

func Chunks[S ~[]E, E any](ts S, chunkSize int) [][]E {
	cs := [][]E{}
	for {
		if len(ts) == 0 {
			break
		}
		copySize := chunkSize
		if len(ts) < copySize {
			copySize = len(ts)
		}
		chunk := make([]E, copySize)
		copy(chunk, ts[:copySize])
		cs = append(cs, chunk)
		ts = ts[copySize:]
	}
	return cs
}
