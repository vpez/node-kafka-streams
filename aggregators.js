const avg = items => {
    let sum = 0;
    for (let i = 0; i < items.length; i++) {
        sum += Number(items[i].value)
    }
    return Math.floor(sum / items.length)
};

const sum = items => {
    let sum = 0;
    for (let i = 0; i < items.length; i++) {
        sum += Number(items[i].value)
    }
    return sum;
};

module.exports = {
    avg: avg,
    sum: sum
};