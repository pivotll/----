// 获取按钮和消息元素
const button = document.getElementById('clickMeBtn');
const message = document.getElementById('message');

// 添加点击事件监听器
button.addEventListener('click', () => {
    // 每次点击时更改消息
    const messages = [
        "部署成功！🎉",
        "你真棒！🚀",
        "Hello Vercel! 🌍",
        "编程很有趣！💻"
    ];
    
    // 随机选择一条消息
    const randomMessage = messages[Math.floor(Math.random() * messages.length)];
    
    // 显示消息
    message.textContent = randomMessage;
    
    // 简单的控制台输出，用于调试
    console.log(`用户点击了按钮，显示消息: ${randomMessage}`);
});
