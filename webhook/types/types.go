package types

import (
	"regexp"
	"strings"
)

func PascalToKebab(input string) string {
	// Use a regex to insert a hyphen before each uppercase letter, except the first one.
	re := regexp.MustCompile("([a-z])([A-Z])")
	hyphenated := re.ReplaceAllString(input, "$1-$2")

	// Convert the result to lowercase.
	return strings.ToLower(hyphenated)
}
