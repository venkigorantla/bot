

class Indicators:

     def ema(self, data, period, previous_ema=None):
        output = {}
        index = 0
        multiplier = 2.0 / (period + 1)
        if(previous_ema is None):
            if len(data) < period:
                return None
            previous_ema = round((sum(data[:period]) / period), 4)
            index = period
        for value in data[index:]:
            #current_ema = (multiplier * value) + ((1 - multiplier) * previous_ema)
            previous_ema = round((((value - previous_ema) * multiplier) + previous_ema), 4)
            #print(value, current_ema)
            output[format(value, "0.10f")] = previous_ema
        return output