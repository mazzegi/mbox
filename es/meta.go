package es

type MetaData map[string]interface{}

func (m *MetaData) Add(typ string, value interface{}) {
	(*m)[typ] = value
}

func (md MetaData) User() string {
	if u, ok := md["user"]; ok {
		if u, ok := u.(string); ok {
			return u
		}
	}
	return ""
}

func UserMeta(user string) MetaData {
	return MetaData{
		"user": user,
	}
}
