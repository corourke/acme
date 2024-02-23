## dbt project to create acme_retail_silver datalake

### Set up athena driver and dbt

```bash
pip3 install dbt-athena-community
```

- You will need to have the Amazon CLI configured and authorized
- Create these S3 buckets:
  - s3://acme-retail-data/datalake/
  - s3://acme-retail-data/dbt/staging/
- Copy the profiles.yml to your home directory: `~/.dbt/profiles.yml`

Then you can run either:

```
dbt debug
dbt run
```

### dbt Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
