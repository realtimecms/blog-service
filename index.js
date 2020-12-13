const App = require("@live-change/framework")
const app = new App()

const validators = require("../validation")

const definition = app.createServiceDefinition({
  name: "blog",
  validators
})

const User = definition.foreignModel('users', 'User')
const Category = definition.foreignModel('categories', 'Category')
const Picture = definition.foreignModel('pictures', 'Picture')
const Tag = definition.foreignModel('tags', 'Tag')

const postFields = {
  date: {
    type: Date,
    validation: ['nonEmpty']
  },
  title: {
    type: String,
    validation: ['nonEmpty']
  },
  content: {
    type: String,
    validation: ['nonEmpty']
  },
  picture: {
    type: Picture,
    validation: ['nonEmpty']
  },
  category: {
    type: Array,
    of: {
      type: Category,
      validation: ['nonEmpty']
    },
    defaultValue: [],
    validation: ['nonEmpty', {name: 'minLength', length: 1}, 'elementsNonEmpty'],
    editor: ['categorySelect'],
    parentCategory: 'post',
    search: {
      type: 'keyword'
    }
  },
  lists: {
    type: Array,
    of: {
      type: String,
      validation: ['nonEmpty']
    },
    defaultValue: [],
    editor: ['multiCheckbox'],
    options: ['top', 'news', 'big-news']
  },
  tags: {
    type: Array,
    of: {
      type: Tag,
      validation: ['nonEmpty'],
      editor: 'relationSingleSelect'
    },
    search: {
      type: 'keyword'
    }
  },
  lang: {type: String, validation: ['nonEmpty']}
}

const Post = definition.model({
  name: "Post",
  properties: {
    slug: {
      type: String
    },
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
    userPostsWithDate: {
      property: ['author', 'date']
    },
    listPostsWithDate: {
      function: async (input, output, { table }) => {
        await input.table("blog_Post").onChange((obj, oldObj) => {
          if(obj && oldObj) {
            let pointers = obj && new Set( obj.lists
                && obj.lists.map(p => JSON.stringify(p)+':'+JSON.stringify(obj.date)))
            let oldPointers = oldObj && new Set( oldObj.lists
                && oldObj.lists.map(p => JSON.stringify(p)+':'+JSON.stringify(oldObj.date)))
            for(let pointer of pointers) {
              if(!oldPointers.has(pointer)) output.change(
                  { id: pointer+'_'+obj.id, to: obj.id }, null)
            }
            for(let pointer of oldPointers) {
              if(!pointers.has(pointer)) output.change(
                  null, { id: pointer+'_'+obj.id, to: obj.id })
            }
          } else if(obj) {
            obj.lists && obj.lists.forEach(p => output.change(
                { id: JSON.stringify(p)+':'+JSON.stringify(obj.date)+'_'+obj.id, to: obj.id }, null))
          } else if(oldObj) {
            oldObj.lists && oldObj.lists.forEach(p => output.change(
                null, { id: JSON.stringify(p)+':'+JSON.stringify(oldObj.date)+'_'+oldObj.id, to: oldObj.id }))
          }
        })
      }
    }
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

    const slug = await service.triggerService('slugs', {
      type: "CreateSlug",
      group: "blog_post",
      title: params.title,
      to: post
    })

    data.slug = slug

    emit({
      type: 'PostCreated',
      post, data
    })

    return { post, slug }
  }
})


definition.view({
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
    return Post.indexRangePath('categoryPostsWithDate', range)
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
    return Post.indexRangePath('userPostsWithDate', range)
  }
})

definition.view({
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
    return Post.indexRangePath('tagPostsWithDate', range)
  }
})

definition.view({
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