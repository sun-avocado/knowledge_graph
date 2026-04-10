# 使用官方 Bun 镜像
FROM oven/bun:latest

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY package.json bun.lock ./

# 安装生产环境依赖
RUN bun install --production

# 复制源代码
COPY . .

# 暴露端口 (默认 3000)
EXPOSE 3000

# 启动命令
CMD ["bun", "run", "index.ts"]
