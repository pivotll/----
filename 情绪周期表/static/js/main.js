// 全局变量
let dataTable = null;
let colorConfig = {};

// 趋势配置：哪些列是“数值越小越好”
// 其他未列出的数值列，默认“数值越大越好”
const trendConfig = {
    down_count: 'smaller',        // 下跌家数
    limit_down_count: 'smaller',  // 跌停数
    break_count: 'smaller',       // 炸板数
    break_rate: 'smaller',        // 炸板率
    down5_count: 'smaller'        // 跌5%数量
};

// 页面加载完成后执行
$(document).ready(function() {
    // 初始化
    initDatePicker();
    loadColorConfig();
    loadStats();
    loadData();
});

// 初始化日期选择器
function initDatePicker() {
    const today = new Date().toISOString().split('T')[0];
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    $('#endDate').val(today);
    $('#startDate').val(thirtyDaysAgo);
}

// 加载颜色配置
function loadColorConfig() {
    $.get('/api/color_config', function(response) {
        if (response.success) {
            colorConfig = response.config;
        }
    });
}

// 加载统计信息
function loadStats() {
    $.get('/api/stats', function(response) {
        if (response.success) {
            $('#dataStats').html(`
                数据范围：${response.min_date} 至 ${response.max_date} 
                （共 ${response.total_days} 个交易日）
            `);
        }
    });
}

// 加载数据
function loadData() {
    const startDate = $('#startDate').val();
    const endDate = $('#endDate').val();
    
    // 显示加载中
    if (dataTable) {
        dataTable.destroy();
    }
    
    $('#tableBody').html('<tr><td colspan="29" class="loading">数据加载中...</td></tr>');
    
    // 请求数据
    $.get('/api/data', {
        start_date: startDate,
        end_date: endDate
    }, function(response) {
        if (response.success) {
            renderTable(response.data);
        } else {
            $('#tableBody').html(`<tr><td colspan="26" class="loading">${response.message}</td></tr>`);
        }
    }).fail(function() {
        $('#tableBody').html('<tr><td colspan="26" class="loading">数据加载失败</td></tr>');
    });
}

// 渲染表格
function renderTable(data) {
    const tbody = $('#tableBody');
    tbody.empty();
    
    if (data.length === 0) {
        tbody.html('<tr><td colspan="29" class="loading">暂无数据</td></tr>');
        return;
    }
    
    // 渲染数据（颜色：当前行与下一行对比，第一行与第二行比故有颜色，最后一行无下一行故无颜色）
    data.forEach((row, rowIndex) => {
        const tr = $('<tr></tr>');
        const nextRow = rowIndex < data.length - 1 ? data[rowIndex + 1] : null;
        
        // 列定义
        const columns = [
            { key: 'trade_date', type: 'text' },
            { key: 'up_count', type: 'number' },
            { key: 'down_count', type: 'number' },
            { key: 'limit_up_count', type: 'number' },
            { key: 'limit_down_count', type: 'number' },
            { key: 'break_count', type: 'number' },
            { key: 'break_rate', type: 'percent', color: true, reverse: true },
            { key: 'first_board', type: 'number' },
            { key: 'second_board', type: 'number' },
            { key: 'third_board', type: 'number' },
            { key: 'above_third', type: 'number' },
            { key: 'max_board', type: 'number' },
            { key: 'advance_1to2', type: 'percent', color: true },
            { key: 'advance_2to3', type: 'percent', color: true },
            { key: 'advance_3to4', type: 'percent', color: true },
            { key: 'advance_3plus', type: 'percent', color: true },
            { key: 'fanpao_count', type: 'number' },
            { key: 'limit_amount', type: 'number' },
            { key: 'seal_amount', type: 'number' },
            { key: 'up5_count', type: 'number' },
            { key: 'down5_count', type: 'number' },
            { key: 'first_red_rate', type: 'percent', color: true },
            { key: 'first_premium', type: 'percent', color: true },
            { key: 'second_red_rate', type: 'percent', color: true },
            { key: 'second_premium', type: 'percent', color: true },
            { key: 'third_red_rate', type: 'percent', color: true },
            { key: 'third_premium', type: 'percent', color: true },
            { key: 'third_plus_red_rate', type: 'percent', color: true },
            { key: 'third_plus_premium', type: 'percent', color: true }
        ];
        
        columns.forEach(col => {
            const td = $('<td></td>');
            const value = row[col.key];
            
            // 显示值
            if (value === null || value === undefined || value === 'None') {
                td.text('-');
            } else if (col.type === 'percent') {
                // 百分比统一保留1位小数
                const num = parseFloat(value);
                td.text(isNaN(num) ? '-' : num.toFixed(1));
            } else if (col.type === 'number') {
                // 数值列：整数原样显示，带小数的保留1位小数
                const num = parseFloat(value);
                if (isNaN(num)) {
                    td.text(value);
                } else if (Number.isInteger(num)) {
                    td.text(num);
                } else {
                    td.text(num.toFixed(1));
                }
            } else {
                td.text(value);
            }
            
            // 环比颜色标记：和下一行比较（第一行有颜色，最后一行无下一行故无颜色）
            const num = parseFloat(value);
            if (!isNaN(num) && nextRow != null) {
                const nextVal = parseFloat(nextRow[col.key]);
                if (!isNaN(nextVal)) {
                    const trend = trendConfig[col.key] || 'bigger';
                    if (trend === 'smaller') {
                        // 越小越好：本行比下一行小 → 红；比下一行大 → 绿
                        if (num < nextVal) {
                            td.addClass('cell-red-light');
                        } else if (num > nextVal) {
                            td.addClass('cell-green-light');
                        }
                    } else {
                        // 越大越好：本行比下一行大 → 红；比下一行小 → 绿
                        if (num > nextVal) {
                            td.addClass('cell-red-light');
                        } else if (num < nextVal) {
                            td.addClass('cell-green-light');
                        }
                    }
                }
            }
            
            tr.append(td);
        });
        
        tbody.append(tr);
    });
    
    // 初始化DataTables（固定顺序：日期从小到大，关闭排序）
    dataTable = $('#emotionTable').DataTable({
        paging: true,
        pageLength: 20,
        lengthMenu: [[10, 20, 50, 100, -1], [10, 20, 50, 100, "全部"]],
        searching: true,
        ordering: false,           // 关闭所有列排序功能
        order: [[0, 'asc']],       // 日期从小到大（即使ordering=false，也保留语义）
        language: {
            "lengthMenu": "显示 _MENU_ 条记录",
            "zeroRecords": "没有找到记录",
            "info": "第 _PAGE_ 页 ( 总共 _PAGES_ 页 )",
            "infoEmpty": "无记录",
            "infoFiltered": "(从 _MAX_ 条记录过滤)",
            "search": "搜索:",
            "paginate": {
                "first": "首页",
                "last": "尾页",
                "next": "下一页",
                "previous": "上一页"
            }
        }
    });
}

// 获取单元格颜色
function getCellColor(value, key, reverse = false) {
    const config = colorConfig[key];
    if (!config) return null;
    
    const goodThreshold = config.good;
    const badThreshold = config.bad;
    const isReverse = config.reverse || reverse;
    
    if (isReverse) {
        // 反向逻辑：值越小越好
        if (value <= goodThreshold) {
            return 'cell-red-light';  // 好
        } else if (value >= badThreshold) {
            return 'cell-green-light';  // 差
        }
    } else {
        // 正向逻辑：值越大越好
        if (value >= goodThreshold) {
            return 'cell-red-light';  // 好
        } else if (value <= badThreshold) {
            return 'cell-green-light';  // 差
        }
    }
    
    return null;
}

// 导出Excel
function exportExcel() {
    const startDate = $('#startDate').val();
    const endDate = $('#endDate').val();
    
    window.location.href = `/api/export?start_date=${startDate}&end_date=${endDate}`;
}

// 触发数据更新
function updateData(mode) {
    const $status = $('#updateStatus');
    const $buttons = $('.update-btn');
    
    let payload = { mode: mode || 'incremental' };
    
    if (mode === 'range') {
        const start = $('#updStartDate').val();
        const end = $('#updEndDate').val();
        
        if (!start || !end) {
            $status.text('请先选择自定义起止日期');
            return;
        }
        
        // 转为YYYYMMDD格式
        payload.start = start.replace(/-/g, '');
        payload.end = end.replace(/-/g, '');
    }
    
    $status.text('正在更新数据，请稍候...');
    $buttons.prop('disabled', true);
    
    $.ajax({
        url: '/api/update',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(payload),
        success: function(res) {
            if (res.success) {
                const range = res.data_range || {};
                const rangeText = (range.min_date && range.max_date)
                    ? `（当前数据范围：${range.min_date} ~ ${range.max_date}）`
                    : '';
                $status.text(res.message + ' ' + rangeText);
                
                // 更新统计和表格
                loadStats();
                loadData();
            } else {
                $status.text(res.message || '更新失败');
            }
        },
        error: function(err) {
            $status.text('更新失败，请查看后台日志');
        },
        complete: function() {
            $buttons.prop('disabled', false);
        }
    });
}
