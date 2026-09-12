# GitHub 开源发布指南

本指南面向第一次使用 GitHub 的用户，说明如何把当前项目发布为公开仓库。

## 1. 发布前确认

### 论文 PDF

学术论文可能包含姓名、学号、学校、联系方式或尚未允许公开的内容。默认情况下，本仓库不会提交根目录或 `docs/paper/` 下的 PDF。

如需公开论文，请先确认以下事项：

- 你拥有论文的公开传播权，且投稿或出版协议允许公开该版本。
- 论文已删除姓名、学号、电话、邮箱、导师信息等个人数据，或者你明确同意公开这些信息。
- 论文使用合适的许可证。若是自著论文，通常可单独使用 CC BY 4.0；代码仍使用代码许可证。

确认可以公开后，删除 `.gitignore` 中的 PDF 排除规则，再执行 `git add`。

### 许可证

公开可见不等于开源。没有 `LICENSE` 时，其他人通常不能合法地复制、修改或分发你的代码。

本项目推荐使用 MIT License。可以在第一次推送后在 GitHub 网页上添加：

1. 打开仓库页面。
2. 选择 `Add file` -> `Create new file`。
3. 文件名填写 `LICENSE`。
4. 页面出现许可证模板入口后选择 MIT License。
5. 提交后，在本地执行 `git pull` 同步。

### 敏感信息

- 不要提交 `.env`、访问令牌、云平台密钥、数据库正式密码或私人数据。
- 本项目当前只有本地演示密码，并已通过 `.env.example` 说明。不要把这些演示密码用于真实环境。
- 建议启用 GitHub 两步验证和 Dependabot。

## 2. 配置 Git 身份

在 PowerShell 中执行：

```powershell
git config --global user.name "你的 GitHub 用户名"
git config --global user.email "你的 GitHub noreply 邮箱"
git config --global init.defaultBranch main
```

建议使用 GitHub 提供的私密邮箱。获取方式：

1. 打开 GitHub 的 `Settings` -> `Emails`。
2. 启用 `Keep my email addresses private`。
3. 复制形如 `12345678+你的用户名@users.noreply.github.com` 的地址。

## 3. 创建本地 Git 仓库

```powershell
git init
git add .
git status --short
```

在 `git status --short` 输出中再次确认：

- 不包含 `.env`；
- 不包含论文 PDF（除非你明确决定公开）；
- 不包含 `node_modules`、运行日志、数据文件或本地生成目录。

确认后提交：

```powershell
git commit -m "Initial public release"
```

## 4. 创建 GitHub 仓库

在 GitHub 页面右上角选择 `+` -> `New repository`：

- `Repository name` 推荐填写 `lakehouse-sales-analytics`。
- `Description` 可以填写：
  `Single-node lakehouse sales analytics demo with Kafka, Spark, Iceberg, ClickHouse, Metabase, FastAPI, and Vue 3.`
- 第一次发布建议选择 `Private`，便于检查。
- 不要勾选 `Add a README file`、`.gitignore` 或许可证，因为本地已经有文件。

创建后，GitHub 会显示仓库地址。

## 5. 推送代码

把下面的用户名和仓库名替换成实际值：

```powershell
git remote add origin https://github.com/你的用户名/lakehouse-sales-analytics.git
git push -u origin main
```

首次推送时，按照 Git Credential Manager 弹出的浏览器页面登录 GitHub。GitHub 已不支持使用账户登录密码进行 Git 推送。

## 6. 公开发布

先在私有仓库中检查：

- README 和图片能否正常显示；
- 文件列表和历史记录中没有密钥、私人信息或误传数据；
- MIT License 已添加；
- `docs` 中的链接均可访问。

检查完成后，打开仓库 `Settings` -> `General` -> 页面底部 `Danger Zone` -> `Change repository visibility`，将仓库改为 `Public`。

最后建议补充：

- Topics：`data-engineering`、`lakehouse`、`apache-spark`、`apache-iceberg`、`kafka`、`clickhouse`、`metabase`、`fastapi`、`vue3`、`portfolio-project`。
- Release：创建 `v1.0.0`，说明这是第一个可公开运行和展示的版本。
- About：填写简短描述，并勾选 README、License、Topics 等展示项。

## 7. 后续更新

```powershell
git status
git add .
git commit -m "Describe your changes"
git push
```

如果已经推送了真实密钥，仅删除文件并不安全。应立即在对应平台作废并更换密钥，再清理 Git 历史。
