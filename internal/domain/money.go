package domain

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

var moneyAmountRE = regexp.MustCompile(`^-?\d+\.\d{2}$`)

type Money struct {
	minor    int64
	currency string
}

func ParseMoney(amount, currency string) (Money, error) {
	currency = strings.TrimSpace(strings.ToUpper(currency))
	if len(currency) != 3 {
		return Money{}, fmt.Errorf("%w: invalid currency", ErrInvalidMoney)
	}
	amount = strings.TrimSpace(amount)
	if amount == "" {
		return Money{}, fmt.Errorf("%w: empty amount", ErrInvalidMoney)
	}
	if strings.ContainsAny(amount, "eE") {
		return Money{}, fmt.Errorf("%w: scientific notation", ErrInvalidMoney)
	}
	if !moneyAmountRE.MatchString(amount) {
		return Money{}, fmt.Errorf("%w: invalid decimal format", ErrInvalidMoney)
	}
	negative := strings.HasPrefix(amount, "-")
	if negative {
		amount = amount[1:]
	}
	parts := strings.Split(amount, ".")
	whole := parts[0]
	frac := parts[1]
	if len(frac) != 2 {
		return Money{}, fmt.Errorf("%w: scale must be 2", ErrInvalidMoney)
	}
	var minor int64
	for _, c := range whole {
		if c < '0' || c > '9' {
			return Money{}, fmt.Errorf("%w: invalid digit", ErrInvalidMoney)
		}
		if minor > math.MaxInt64/10 {
			return Money{}, ErrOverflow
		}
		minor = minor*10 + int64(c-'0')
	}
	for _, c := range frac {
		if c < '0' || c > '9' {
			return Money{}, fmt.Errorf("%w: invalid digit", ErrInvalidMoney)
		}
		if minor > math.MaxInt64/10 {
			return Money{}, ErrOverflow
		}
		minor = minor*10 + int64(c-'0')
	}
	if negative {
		return Money{}, fmt.Errorf("%w: negative external amount", ErrInvalidMoney)
	}
	return Money{minor: minor, currency: currency}, nil
}

func MustMoney(amount, currency string) Money {
	m, err := ParseMoney(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}

func ZeroMoney(currency string) Money {
	return Money{minor: 0, currency: strings.ToUpper(currency)}
}

func MoneyFromMinor(minor int64, currency string) Money {
	return Money{minor: minor, currency: strings.ToUpper(currency)}
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() string   { return m.currency }
func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsPositive() bool   { return m.minor > 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }

func (m Money) FormatAmount() string {
	sign := ""
	minor := m.minor
	if minor < 0 {
		sign = "-"
		minor = -minor
	}
	whole := minor / 100
	frac := minor % 100
	return fmt.Sprintf("%s%d.%02d", sign, whole, frac)
}

func (m Money) Add(o Money) (Money, error) {
	if m.currency != o.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if m.minor > math.MaxInt64-o.minor {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if m.currency != o.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if m.minor < o.minor && o.minor-m.minor > math.MaxInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Cmp(o Money) (int, error) {
	if m.currency != o.currency {
		return 0, ErrCurrencyMismatch
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.minor == o.minor
}
