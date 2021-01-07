const App = require("@live-change/framework")
const app = new App()

const validators = require("../validation")

const definition = app.createServiceDefinition({
  name: "blog",
  validators
})

const config = require('../config/blog.js')(definition)

const User = definition.foreignModel('users', 'User')
const Category = definition.foreignModel('categories', 'Category')
const Picture = definition.foreignModel('pictures', 'Picture')
const Slug = definition.foreignModel('slugs', 'Slug')
const Tag = definition.foreignModel('tags', 'Tag')

const postFields = {
  date: {
    type: Date,
    validation: ['nonEmpty']
  },
  slug: {
    type: String
  },
  ...(config.fields)
}

const Post = definition.model({
  name: "Post",
  properties: {
    author: {
      type: User,
      validation: ['nonEmpty']
    },
    ...postFields
  },
  indexes: {
    postsByDate: {
      property: "date",
    },
    ...(postFields.category ? {
      categoryPostsWithDate: {
        function: async (input, output, { table }) => {
          await input.table("blog_Post").onChange((obj, oldObj) => {
            if(obj && oldObj) {
              let pointers = new Set(obj.category
                  && obj.category.map(p => JSON.stringify(p)+':'+JSON.stringify(obj.date)))
              let oldPointers = new Set(oldObj.category
                  && oldObj.category.map(p => JSON.stringify(p)+':'+JSON.stringify(oldObj.date)))
              output.debug("CATEGORY POINTERS", pointers)
              output.debug("OLD CATEGORY POINTERS", oldPointers)
              for(let pointer of pointers) {
                if(!oldPointers.has(pointer)) {
                  output.debug("ADD NEW POINTER", pointers)
                  output.change(
                      { id: pointer+'_'+obj.id, to: obj.id }, null)
                } else {
                  output.debug("IGNORE NEW POINTER", pointers)
                }
              }
              for(let pointer of oldPointers) {
                if(!pointers.has(pointer)) {
                  output.change(
                      null, { id: pointer+'_'+obj.id, to: obj.id })
                }
              }
            } else if(obj) {
              obj.category && obj.category.forEach(p => output.change(
                  { id: JSON.stringify(p)+':'+JSON.stringify(obj.date)+'_'+obj.id, to: obj.id }, null))
            } else if(oldObj) {
              oldObj.category && oldObj.category.forEach(p => output.change(
                  null, { id: JSON.stringify(p)+':'+JSON.stringify(oldObj.date)+'_'+oldObj.id, to: oldObj.id }))
            }
          })
        }
      },
    } : {}),
    ...(postFields.tags ? {
      tagPostsWithDate: {
        function: async (input, output, { table }) => {
          await input.table("blog_Post").onChange((obj, oldObj) => {
            if(obj && oldObj) {
              let pointers = new Set(obj.tags
                  && obj.tags.map(p => JSON.stringify(p)+':'+JSON.stringify(obj.date)))
              let oldPointers = new Set(oldObj.tags &&
                  oldObj.tags.map(p => JSON.stringify(p)+':'+JSON.stringify(oldObj.date)))
              for(let pointer of pointers) {
                if(!oldPointers.has(pointer)) output.change(
                    { id: pointer+'_'+obj.id, to: obj.id }, null)
              }
              for(let pointer of oldPointers) {
                if(!pointers.has(pointer)) output.change(
                    null, { id: pointer+'_'+obj.id, to: obj.id })
              }
            } else if(obj) {
              obj.tags && obj.tags.forEach(p => output.change(
                  { id: JSON.stringify(p)+':'+JSON.stringify(obj.date)+'_'+obj.id, to: obj.id }, null))
            } else if(oldObj) {
              oldObj.tags && oldObj.tags.forEach(p => output.change(
                  null, { id: JSON.stringify(p)+':'+JSON.stringify(oldObj.date)+'_'+oldObj.id, to: oldObj.id }))
            }
          })
        }
      },
    }: {}),
    userPostsWithDate: {
      property: ['author', 'date']
    },
    ...(postFields.lists ? {
      listPostsWithDate: {
        function: async (input, output, { table }) => {
          await input.table("blog_Post").onChange((obj, oldObj) => {
            if(obj && oldObj) {
              let pointers = obj && new Set(obj.lists
                  && obj.lists.map(p => JSON.stringify(p) + ':' + JSON.stringify(obj.date)))
              let oldPointers = oldObj && new Set(oldObj.lists
                  && oldObj.lists.map(p => JSON.stringify(p) + ':' + JSON.stringify(oldObj.date)))
              for(let pointer of pointers) {
                if(!oldPointers.has(pointer)) output.change(
                    { id: pointer + '_' + obj.id, to: obj.id }, null)
              }
              for(let pointer of oldPointers) {
                if(!pointers.has(pointer)) output.change(
                    null, { id: pointer + '_' + obj.id, to: obj.id })
              }
            } else if(obj) {
              obj.lists && obj.lists.forEach(p => output.change(
                  { id: JSON.stringify(p) + ':' + JSON.stringify(obj.date) + '_' + obj.id, to: obj.id }, null))
            } else if(oldObj) {
              oldObj.lists && oldObj.lists.forEach(p => output.change(
                  null, { id: JSON.stringify(p) + ':' + JSON.stringify(oldObj.date) + '_' + oldObj.id, to: oldObj.id }))
            }
          })
        }
      }
    } : {})
  },
  crud: {
    deleteTrigger: true,
    writeOptions: {
      access: (params, {client, service}) => {
        return client.roles.includes('admin')
      }
    }
  }
})

definition.action({
  name: "createPost",
  properties: {
    ...postFields
  },
  waitForEvents: true,
  access: (params, { client }) => {
    return client.roles && client.roles.includes('admin')
  },
  async execute(params, { client, service }, emit) {
    const post = app.generateUid()
    let data = {}
    for (let key in postFields) {
      data[key] = params[key]
    }

    data.author = client.user

    if(!data.slug) {
      data.slug = await service.triggerService('slugs', {
        type: "CreateSlug",
        group: "blog_post",
        title: params.title,
        to: post
      })
    } else {
      try {
        await service.triggerService('slugs', {
          type: "TakeSlug",
          group: "blog_post",
          path: data.slug,
          to: post
        })
      } catch(e) {
        throw { properties: { slug: 'taken' } }
      }
    }

    await Post.create({ ...data, id: post })

    emit({
      type: 'PostCreated',
      post, data
    })

    return { post, slug: data.slug }
  }
})

definition.action({
  name: "PostCreate",
  properties: {
    ...postFields
  },
  access: (params, { client }) => {
    return client.roles && client.roles.includes('admin')
  },
  async execute (params, { client, service }, emit) {
    const post = app.generateUid()
    let data = { }
    for(let key in postFields) {
      data[key] = params[key]
    }

    if(!data.slug) {
      data.slug = await service.triggerService('slugs', {
        type: "CreateSlug",
        group: "blog_post",
        title: params.title,
        to: post
      })
    } else {
      try {
        await service.triggerService('slugs', {
          type: "TakeSlug",
          group: "blog_post",
          path: data.slug,
          to: post
        })
      } catch(e) {
        throw { properties: { slug: 'taken' } }
      }
    }

    emit({
      type: 'PostCreated',
      post, data
    })

    return pos
  }
})


definition.action({
  name: "PostUpdate",
  properties: {
    post: {
      type: String
    },
    ...postFields
  },
  access: (params, { client }) => {
    return client.roles && client.roles.includes('admin')
  },
  async execute (params, { client, service }, emit) {
    let data = { }
    for(let key in postFields) {
      data[key] = params[key]
    }

    console.log("UPDATE POST", params)

    const post = params.post

    let current = await Post.get(post)

    if(current.slug != data.slug) {
      if (!data.slug) {
        data.slug = await service.triggerService('slugs', {
          type: "CreateSlug",
          group: "blog_post",
          title: params.name,
          to: post
        })
      } else {
        try {
          await service.triggerService('slugs', {
            type: "TakeSlug",
            group: "blog_post",
            path: data.slug,
            to: post
          })
        } catch (e) {
          throw { properties: { slug: 'taken' } }
        }
      }
      await service.triggerService('slugs', {
        type: "ReleaseSlug",
        group: "blog_post",
        path: current.slug,
        to: post
      })
    }

    emit({
      type: 'PostUpdated',
      post, data
    })

    return post
  }
})

definition.action({
  name: "PostDelete",
  properties: {
    post: {
      type: String
    }
  },
  access: (params, { client }) => {
    return client.roles && client.roles.includes('admin')
  },
  async execute ({ post }, { client, service }, emit) {
    let current = await Post.get(post)
    await service.triggerService('slugs', {
      type: "ReleaseSlug",
      group: "post",
      path: current.slug,
      to: post
    })
    await service.trigger({
      type: "PostDeleted",
      post
    })
    emit({
      type: 'PostDeleted',
      post
    })
  }
})

if(postFields.category) definition.view({
  name: "postsByCategory",
  properties: {
    category: {
      type: Category,
    },
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Post
    }
  },
  async daoPath({ category, gt, lt, gte, lte, limit, reverse }, { client, service }, method) {
    const prefix = JSON.stringify(category)
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:\x00`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt) + "\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    console.log("postsByCategory Input Range", { category, gt, lt, gte, lte, limit, reverse })
    console.log("postsByCategory Computed Range", range)
    return Post.sortedIndexRangePath('categoryPostsWithDate', range)
  }
})

definition.view({
  name: "postsByUser",
  properties: {
    author: {
      type: User,
    },
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Post
    }
  },
  async daoPath({ user, gt, lt, gte, lte, limit, reverse }, { client, service }, method) {
    const prefix = JSON.stringify(user)
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:\x00`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt) + "\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    return Post.sortedIndexRangePath('userPostsWithDate', range)
  }
})

if(postFields.tags) definition.view({
  name: "postsByTag",
  properties: {
    tag: {
      type: Tag,
    },
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Post
    }
  },
  async daoPath({ tag, gt, lt, gte, lte, limit, reverse }, { client, service }, method) {
    const prefix = JSON.stringify(tag)
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:\x00`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt) + "\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    return Post.sortedIndexRangePath('tagPostsWithDate', range)
  }
})

if(postFields.lists) definition.view({
  name: "postsByList",
  properties: {
    list: {
      type: String,
    },
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Post
    }
  },
  async daoPath({ list, gt, lt, gte, lte, limit, reverse }, { client, service }, method) {
    const prefix = JSON.stringify(list)
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:\x00`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt) + "\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    /*console.log("postsByList Input Range", { list, gt, lt, gte, lte, limit, reverse })
    console.log("postsByList Computed Range", range)*/
    return Post.sortedIndexRangePath('listPostsWithDate', range)
  }
})

definition.view({
  name: "posts",
  properties: {
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Post
    }
  },
  async daoPath({ gt, lt, gte, lte, limit, reverse }, {client, service}, method) {
    const prefix = ""
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt)+"\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    return Post.indexRangePath('postsByDate', range)
  }
})


module.exports = definition

async function start() {
  app.processServiceDefinition(definition, [...app.defaultProcessors])
  await app.updateService(definition)//, { force: true })
  const service = await app.startService(definition, { runCommands: true, handleEvents: true })

  //require("../config/metricsWriter.js")(definition.name, () => ({}))
}

if (require.main === module) start().catch(error => {
  console.error(error)
  process.exit(1)
})

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
})