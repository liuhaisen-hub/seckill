package utils

// ListOptions 是与存储无关的列表查询参数，由 data 层翻译为驱动查询语言。
type ListOptions struct {
	Filters map[string]any
	OrderBy []string
	Offset  int
	Limit   int
}

// ListOption 以函数式选项组装列表查询。
type ListOption func(*ListOptions)

// NewListOptions 应用函数式选项，供 data 层在翻译查询时调用。
func NewListOptions(opts ...ListOption) *ListOptions {
	o := &ListOptions{Filters: make(map[string]any)}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// ListFilter 追加一个等值过滤条件，key 使用领域字段名。
func ListFilter(key string, value any) ListOption {
	return func(o *ListOptions) {
		o.Filters[key] = value
	}
}

// ListOrderBy 追加排序项，如 "created_at DESC"。
func ListOrderBy(field string) ListOption {
	return func(o *ListOptions) {
		o.OrderBy = append(o.OrderBy, field)
	}
}

func ListOffset(offset int) ListOption {
	return func(o *ListOptions) {
		o.Offset = offset
	}
}

func ListLimit(limit int) ListOption {
	return func(o *ListOptions) {
		o.Limit = limit
	}
}
