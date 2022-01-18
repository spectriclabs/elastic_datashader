import pynumeral
import re

def patched_get_python_format(self, numeralfmt, value):
    thousand = "," if "," in numeralfmt else ""
    float_or_exp = "e" if "e+0" in numeralfmt else "f"
    precision = numeralfmt.split(".")[1] if "." in numeralfmt else None
    if re.match("^0{2,}$", numeralfmt.split(",")[0]):
        fmt = "{:0%s%s}" % (
            len(numeralfmt.split(",")[0])
            + (int(math.log10(abs(value)) / 3) if value != 0 else 0)
            + int(value < 0),
            thousand,
        )
        value = int(value)
    else:
        if precision is not None:
            if "[" in numeralfmt and "[.]" not in numeralfmt:
                precision = precision.replace("]", "").split("[")
                split_val = str(value).split(".")
                decimals = 0 if len(split_val) == 1 else precision[1].count("0")
            else:
                decimals = len(
                    list(filter(lambda c: c == "0", precision.replace("e+0", "")))
                )
        else:
            decimals = 0
        plus = "+" if re.match(".*(^|[^e])\\+.*", numeralfmt) else "-"
        fmt = "{:%s%s.%s%s}" % (plus, thousand, decimals, float_or_exp)
    return fmt, value

def patched_get_format(self, numeralfmt, value):
    prefix, value = self.get_prefix(numeralfmt, value)
    suffix, value = self.get_suffix(numeralfmt, value)
    fmt, value = self.get_python_format(numeralfmt, value)
    ret = prefix + fmt.format(value) + suffix
    if numeralfmt.startswith("(") and numeralfmt.endswith(")") and value < 0:
        ret = "(%s)" % ret.replace("-", "")
    if numeralfmt.strip("(").startswith(".") and value > -1 and value < 1:
        ret = ret.replace("0.", ".")
    if ".[" in numeralfmt and "[.]" not in numeralfmt and "." in ret:
        precision = numeralfmt.replace("]", "").split("[")[1].count("0")
        # remove up to precision zeros from the end
        ii = len(ret)
        for ii in reversed(range(len(ret))):
            if ret[ii] != "0":
                break
        if ret[ii] == ".":
            ii -= 1
        ret = ret[0 : ii + 1]
    return ret

pynumeral.pynumeral.BaseFormatter.get_python_format = patched_get_python_format
pynumeral.pynumeral.BaseFormatter.format = patched_get_format
pynumeral.pynumeral.default_formatter = pynumeral.pynumeral.BaseFormatter()

pynumeral.pynumeral.formatters = [
    pynumeral.pynumeral.NoneFormatter(),
    pynumeral.pynumeral.DurationWithSecondsFormatter(),
    pynumeral.pynumeral.DurationWithoutSecondsFormatter(),
    pynumeral.pynumeral.OrderFormatter(),
    pynumeral.pynumeral.BinaryBytesFormatter(),
    pynumeral.pynumeral.DecimalBytesFormatter(),
    pynumeral.pynumeral.PercentageFormatter(),
    pynumeral.pynumeral.HumanFormatter(),
]
