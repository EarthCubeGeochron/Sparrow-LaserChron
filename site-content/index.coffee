import {Markdown} from '@macrostrat/ui-components'
import aboutText from './about.md'
import h from 'react-hyperscript'
import {DetritalZirconComponent, DZSessionData} from 'plugins/dz-spectrum'
import {AggregateHistogram} from 'plugins/dz-aggregate-histogram'
import ReactJson from "react-json-view"

export default {
  landingText: h Markdown, {src: aboutText}
  landingGraphic: h AggregateHistogram
  siteTitle: 'Arizona LaserChron Center'
  sessionDetailTabs: [{
    title: "Detrital zircon"
    id: "detrital-zircon"
    component: DetritalZirconComponent
  }],
  sessionCardContent: ({data})->h(DZSessionData, data)
}
