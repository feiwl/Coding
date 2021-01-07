# -*- coding: utf-8 -*-

from decimal import Decimal, _WorkRep


class FDecimal(Decimal):
    """
    Replacement for Decimal that allows for arithmetic with float values
    """

    # The __new__ method was copied from CPython source code.
    # Copyright (c) 2004 Python Software Foundation.
    # All rights reserved.
    def __new__(cls, value="0", context=None):
        """Create a decimal point instance.

        >>> FDecimal('3.14')              # string input
        FDecimal('3.14')
        >>> FDecimal((0, (3, 1, 4), -2))  # tuple (sign, digit_tuple, exponent)
        FDecimal('3.14')
        >>> FDecimal(314)                 # int or long
        FDecimal('314')
        >>> FDecimal(Decimal(314))        # another decimal instance
        FDecimal('314')
        >>> FDecimal('  3.14  \\n')        # leading and trailing whitespace okay
        FDecimal('3.14')
        """

        # Note that the coefficient, self._int, is actually stored as
        # a string rather than as a tuple of digits.  This speeds up
        # the "digits to integer" and "integer to digits" conversions
        # that are used in almost every arithmetic operation on
        # Decimals.  This is an internal detail: the as_tuple function
        # and the Decimal constructor still deal with tuples of
        # digits.

        self = object.__new__(cls)

        # From a string
        # REs insist on real strings, so we can too.
        if isinstance(value, basestring):
            m = _parser(value.strip())
            if m is None:
                if context is None:
                    context = getcontext()
                return context._raise_error(ConversionSyntax,
                                "Invalid literal for Decimal: %r" % value)

            if m.group('sign') == "-":
                self._sign = 1
            else:
                self._sign = 0
            intpart = m.group('int')
            if intpart is not None:
                # finite number
                fracpart = m.group('frac') or ''
                exp = int(m.group('exp') or '0')
                self._int = str(int(intpart+fracpart))
                self._exp = exp - len(fracpart)
                self._is_special = False
            else:
                diag = m.group('diag')
                if diag is not None:
                    # NaN
                    self._int = str(int(diag or '0')).lstrip('0')
                    if m.group('signal'):
                        self._exp = 'N'
                    else:
                        self._exp = 'n'
                else:
                    # infinity
                    self._int = '0'
                    self._exp = 'F'
                self._is_special = True
            return self

        # From an integer
        if isinstance(value, (int,long)):
            if value >= 0:
                self._sign = 0
            else:
                self._sign = 1
            self._exp = 0
            self._int = str(abs(value))
            self._is_special = False
            return self

        # From another decimal
        if isinstance(value, Decimal):
            self._exp  = value._exp
            self._sign = value._sign
            self._int  = value._int
            self._is_special  = value._is_special
            return self

        # From an internal working value
        if isinstance(value, _WorkRep):
            self._sign = value.sign
            self._int = str(value.int)
            self._exp = int(value.exp)
            self._is_special = False
            return self

        # tuple/list conversion (possibly from as_tuple())
        if isinstance(value, (list,tuple)):
            if len(value) != 3:
                raise ValueError('Invalid tuple size in creation of Decimal '
                                 'from list or tuple.  The list or tuple '
                                 'should have exactly three elements.')
            # process sign.  The isinstance test rejects floats
            if not (isinstance(value[0], (int, long)) and value[0] in (0,1)):
                raise ValueError("Invalid sign.  The first value in the tuple "
                                 "should be an integer; either 0 for a "
                                 "positive number or 1 for a negative number.")
            self._sign = value[0]
            if value[2] == 'F':
                # infinity: value[1] is ignored
                self._int = '0'
                self._exp = value[2]
                self._is_special = True
            else:
                # process and validate the digits in value[1]
                digits = []
                for digit in value[1]:
                    if isinstance(digit, (int, long)) and 0 <= digit <= 9:
                        # skip leading zeros
                        if digits or digit != 0:
                            digits.append(digit)
                    else:
                        raise ValueError("The second value in the tuple must "
                                         "be composed of integers in the range "
                                         "0 through 9.")
                if value[2] in ('n', 'N'):
                    # NaN: digits form the diagnostic
                    self._int = ''.join(map(str, digits))
                    self._exp = value[2]
                    self._is_special = True
                elif isinstance(value[2], (int, long)):
                    # finite number: digits give the coefficient
                    self._int = ''.join(map(str, digits or [0]))
                    self._exp = value[2]
                    self._is_special = False
                else:
                    raise ValueError("The third value in the tuple must "
                                     "be an integer, or one of the "
                                     "strings 'F', 'n', 'N'.")
            return self

        if isinstance(value, float):
            value = Decimal.from_float(value)
            self._exp  = value._exp
            self._sign = value._sign
            self._int  = value._int
            self._is_special  = value._is_special
            return self

        raise TypeError("Cannot convert %r to Decimal" % value)

    def __repr__(self):
        """Represents the number as an instance of Decimal."""
        # Invariant:  eval(repr(d)) == d
        return "FDecimal('%s')" % str(self)

    def __add__(self, other, context=None):
        if isinstance(other, float):
            other = Decimal(other)
        return FDecimal(super(FDecimal, self).__add__(other, context))
    __radd__ = __add__

    def __sub__(self, other, context=None):
        if isinstance(other, float):
            other = Decimal(other)
        return FDecimal(super(FDecimal, self).__sub__(other, context))
    __rsub__ = __sub__

    def __mul__(self, other, context=None):
        if isinstance(other, float):
            other = Decimal(other)
        return FDecimal(super(FDecimal, self).__mul__(other, context))
    __rmul__ = __mul__

    def __truediv__(self, other, context=None):
        if isinstance(other, float):
            other = Decimal(other)
        return FDecimal(super(FDecimal, self).__truediv__(other, context))
    __rtruediv__ = __truediv__
    __div__ = __truediv__
    __rdiv__ = __rtruediv__
